package com.sequenceiq.cloudbreak.client

import groovy.json.JsonBuilder
import groovy.text.SimpleTemplateEngine
import groovy.text.TemplateEngine
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient

@Slf4j
class CloudbreakClient {

    def private enum Resource {
        USER_CREDENTIALS("user/credentials", "credentials.json"),
        USER_CREDENTIALS_EC2("user/credentials", "credentials_ec2.json"),
        USER_CREDENTIALS_AZURE("user/credentials", "credentials_azure.json"),
        USER_CREDENTIALS_AZURE_RM("user/credentials", "credentials_azure_rm.json"),
        USER_CREDENTIALS_GCP("user/credentials", "credentials_gcp.json"),
        USER_CREDENTIALS_OPENSTACK("user/credentials", "credentials_openstack.json"),
        ACCOUNT_CREDENTIALS("account/credentials", "credentials.json"),
        ACCOUNT_CREDENTIALS_EC2("account/credentials", "credentials_ec2.json"),
        ACCOUNT_CREDENTIALS_OPENSTACK("account/credentials", "credentials_openstack.json"),
        ACCOUNT_CREDENTIALS_AZURE("account/credentials", "credentials_azure.json"),
        ACCOUNT_CREDENTIALS_AZURE_RM("account/credentials", "credentials_azure_rm.json"),
        ACCOUNT_CREDENTIALS_GCP("account/credentials", "credentials_gcp.json"),
        GLOBAL_CREDENTIALS("credentials", ""),
        USER_TEMPLATES("user/templates", "template.json"),
        USER_TEMPLATES_EC2("user/templates", "template_ec2.json"),
        USER_TEMPLATES_EC2_SPOT("user/templates", "template_spot_ec2.json"),
        USER_TEMPLATES_GCP("user/templates", "template_gcp.json"),
        USER_TEMPLATES_AZURE("user/templates", "template_azure.json"),
        USER_TEMPLATES_OPENSTACK("user/templates", "template_openstack.json"),
        ACCOUNT_TEMPLATES("account/templates", "template.json"),
        ACCOUNT_TEMPLATES_EC2("account/templates", "template_ec2.json"),
        ACCOUNT_TEMPLATES_EC2_SPOT("account/templates", "template_spot_ec2.json"),
        ACCOUNT_TEMPLATES_AZURE("account/templates", "template_azure.json"),
        ACCOUNT_TEMPLATES_GCP("account/templates", "template_gcp.json"),
        ACCOUNT_TEMPLATES_OPENSTACK("account/templates", "template_openstack.json"),
        GLOBAL_TEMPLATES("templates", ""),
        USER_STACKS("user/stacks", "stack.json"),
        ACCOUNT_STACKS("account/stacks", "stack.json"),
        ACCOUNT_STACKS_WITH_IMAGE("account/stacks", "stackimage.json"),
        USER_STACKS_WITH_IMAGE("user/stacks", "stackimage.json"),
        GLOBAL_STACKS_NODECOUNT_PUT("stacks", "stack_nodecount_put.json"),
        GLOBAL_STACKS_STATUS_PUT("stacks", "stack_status_put.json"),
        GLOBAL_STACKS("stacks", ""),
        GLOBAL_STACKS_PLATFORM_VARIANT("stacks/platformVariants", ""),
        GLOBAL_CONNECTOR_PARAMS("connectors", ""),
        STACK_AMBARI("stacks/ambari", ""),
        USER_BLUEPRINTS("user/blueprints", "blueprint.json"),
        ACCOUNT_BLUEPRINTS("account/blueprints", "blueprint.json"),
        GLOBAL_BLUEPRINTS("blueprints", "blueprint.json"),
        CLUSTER_NODECOUNT_PUT("stacks/stack-id/cluster", "cluster_nodecount_put.json"),
        CLUSTER_STATUS_PUT("stacks/stack-id/cluster", "cluster_status_put.json"),
        CLUSTERS("stacks/stack-id/cluster", "cluster.json"),
        CERTIFICATES("credentials/certificate", "certificate.json"),
        ACCOUNT_RECIPES("account/recipes", "recipe.json"),
        USER_RECIPES("user/recipes", "recipe.json"),
        GLOBAL_RECIPES("recipes", ""),
        USER_NETWORKS("user/networks", ""),
        USER_NETWORKS_AWS("user/networks", "networks_aws.json"),
        USER_NETWORKS_AZURE("user/networks", "networks_azure.json"),
        USER_NETWORKS_GCP("user/networks", "networks_gcp.json"),
        USER_NETWORKS_OPENSTACK("user/networks", "networks_openstack.json"),
        ACCOUNT_NETWORKS("account/networks", ""),
        ACCOUNT_NETWORKS_AWS("account/networks", "networks_aws.json"),
        ACCOUNT_NETWORKS_AZURE("account/networks", "networks_azure.json"),
        ACCOUNT_NETWORKS_OPENSTACK("account/networks", "networks_openstack.json"),
        ACCOUNT_NETWORKS_GCP("account/networks", "networks_gcp.json"),
        GLOBAL_NETWORKS("networks", ""),
        USER_SECURITY_GROUPS("user/securitygroups", ""),
        ACCOUNT_SECURITY_GROUPS("account/securitygroups", ""),
        GLOBAL_SECURITY_GROUPS("securitygroups", "")

        def path
        def template

        Resource(path, template) {
            this.path = path
            this.template = template
        }

        def String path() {
            return this.path
        }

        def String template() {
            return this.template
        }
    }

    def RESTClient restClient;
    def TemplateEngine engine = new SimpleTemplateEngine()

    CloudbreakClient(String address, token) {
        address = address.endsWith("/") ? address : address + "/";
        restClient = new RESTClient(address as String)
        restClient.client.params.setParameter("http.socket.timeout", new Integer(60000))
        restClient.client.params.setParameter("http.connection.timeout", new Integer(60000))
        restClient.ignoreSSLIssues();
        restClient.headers['Authorization'] = 'Bearer ' + token
    }

    def String postStack(String stackName, String credentialId, String region, Boolean publicInAccount, Map<String, Object> instanceGroupTemplates,
                         String onFailure, Long threshold, String adjustmentType, String image = null, String networkId, String securityGroupId, Integer diskPerStorage = null,
                         Boolean dedicatedInstances = null, String platformVariant = "", String availabilityZone = null) throws Exception {
        log.debug("Posting stack ...")
        StringBuilder group = new StringBuilder();
        for (Map.Entry<String, Object> map : instanceGroupTemplates.entrySet()) {
            group.append("{");
            group.append(String.format("\"templateId\": %s, \"group\": \"%s\", \"nodeCount\": %s, \"type\": \"%s\"", map.getValue().templateId, map.getKey(), map.getValue().nodeCount, map.getValue().type));
            group.append("},");
        }
        def response;
        def params = diskPerStorage == null ? [:] : ["diskPerStorage": diskPerStorage]
        if (dedicatedInstances) {
            params << ["dedicatedInstances": dedicatedInstances]
        };

        if (image == null || image == "") {
            def binding = ["STACK_NAME"      : stackName,
                           "CREDENTIAL_ID"   : credentialId,
                           "REGION"          : region,
                           "ON_FAILURE"      : onFailure,
                           "AVAILABILITYZONE": availabilityZone,
                           "THRESHOLD"       : threshold,
                           "ADJUSTMENTTYPE"  : adjustmentType,
                           "GROUPS"          : group.toString().substring(0, group.toString().length() - 1),
                           "NETWORK_ID"      : networkId,
                           "SECURITY_GROUP"  : securityGroupId,
                           "PLATFORM_VARIANT": platformVariant ? platformVariant : "",
                           "PARAMETERS"      : new JsonBuilder(params).toPrettyString()]
            if (publicInAccount) {
                response = processPost(Resource.ACCOUNT_STACKS, binding)
            } else {
                response = processPost(Resource.USER_STACKS, binding)
            }
        } else {
            def binding = ["STACK_NAME"      : stackName,
                           "CREDENTIAL_ID"   : credentialId,
                           "REGION"          : region,
                           "AVAILABILITYZONE": availabilityZone,
                           "IMAGE"           : image,
                           "ON_FAILURE"      : onFailure,
                           "THRESHOLD"       : threshold,
                           "ADJUSTMENTTYPE"  : adjustmentType,
                           "GROUPS"          : group.toString().substring(0, group.toString().length() - 1),
                           "NETWORK_ID"      : networkId,
                           "SECURITY_GROUP"  : securityGroupId,
                           "PLATFORM_VARIANT": platformVariant ? platformVariant : "",
                           "PARAMETERS"      : new JsonBuilder(params).toPrettyString()]
            if (publicInAccount) {
                response = processPost(Resource.ACCOUNT_STACKS_WITH_IMAGE, binding)
            } else {
                response = processPost(Resource.USER_STACKS_WITH_IMAGE, binding)
            }
        }

        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postBlueprint(String name, String description, String blueprint, Boolean publicInAccount) throws Exception {
        log.debug("Posting blueprint ...")
        def binding = ["BLUEPRINT": blueprint, "NAME": name, "DESCRIPTION": description]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_BLUEPRINTS, binding)
        } else {
            response = processPost(Resource.USER_BLUEPRINTS, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postRecipe(String recipe, Boolean publicInAccount) throws Exception {
        log.debug("Posting recipe to account...")
        def binding = ["RECIPE": recipe]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_RECIPES, binding)
        } else {
            response = processPost(Resource.USER_RECIPES, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postEc2Credential(String name, String description, String roleArn, String sshKey, Boolean publicInAccount) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "ROLE_ARN": roleArn, "DESCRIPTION": description, "SSHKEY": sshKey]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_CREDENTIALS_EC2, binding)
        } else {
            response = processPost(Resource.USER_CREDENTIALS_EC2, binding)
        }
        return response?.data?.id
    }

    def String postOpenStackCredential(String name, String description, String userName, String password, String tenantName, String endPoint, String sshKey, Boolean publicInAccount) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "OPENSTACK", "NAME": name, "USERNAME": userName, "PASSWORD": password, "TENANTNAME": tenantName, "ENDPOINT": endPoint, "DESCRIPTION": description, "SSHKEY": sshKey]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_CREDENTIALS_OPENSTACK, binding)
        } else {
            response = processPost(Resource.USER_CREDENTIALS_OPENSTACK, binding)
        }
        return response?.data?.id
    }

    def String postGcpCredential(String name, String description, String sshKey, Boolean publicInAccount, String projectId, String serviceAccountId, String serviceAccountPrivateKey) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "GCP", "NAME": name, "PROJECT_ID": projectId, "DESCRIPTION": description, "SSHKEY": sshKey, "SERVICE_ACCOUNT_ID": serviceAccountId, "SERVICE_ACCOUNT_PRIVATE_KEY": serviceAccountPrivateKey]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_CREDENTIALS_GCP, binding)
        } else {
            response = processPost(Resource.USER_CREDENTIALS_GCP, binding)
        }
        return response?.data?.id
    }

    def String postAzureCredential(String name, String description, String subscriptionId, String sshKey, Boolean publicInAccount) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "AZURE", "NAME": name, "DESCRIPTION": description, "SUBSCRIPTIONID": subscriptionId, "SSHKEY": sshKey]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_CREDENTIALS_AZURE, binding)
        } else {
            response = processPost(Resource.USER_CREDENTIALS_AZURE, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postAzureRmCredential(String name, String description, String subscriptionId, String tenantId, String accesKey, String secretKey, String sshKey, Boolean publicInAccount) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "AZURE_RM", "NAME": name, "DESCRIPTION": description, "SUBSCRIPTIONID": subscriptionId, "SECRETKEY": secretKey, "TENANTID": tenantId, "ACCESKEY": accesKey, "SSHKEY": sshKey]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_CREDENTIALS_AZURE_RM, binding)
        } else {
            response = processPost(Resource.USER_CREDENTIALS_AZURE_RM, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String getCertificate(String id) throws Exception {
        return getOne(Resource.CERTIFICATES, id).text
    }

    def String postSpotEc2Template(String name, String description, String sshLocation, String instanceType, String volumeCount, String volumeSize, String volumeType, String spotPrice, Boolean publicInAccount, Boolean encrypted) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "SSH_LOCATION": sshLocation, "INSTANCE_TYPE": instanceType, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "VOLUME_TYPE": volumeType, "ENCRYPTED": encrypted, "SPOT_PRICE": spotPrice]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_EC2_SPOT, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_EC2_SPOT, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postEc2Template(String name, String description, String sshLocation, String instanceType, String volumeCount, String volumeSize, String volumeType, Boolean publicInAccount, Boolean encrypted) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "SSH_LOCATION": sshLocation, "INSTANCE_TYPE": instanceType, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "VOLUME_TYPE": volumeType, "ENCRYPTED": encrypted]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_EC2, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_EC2, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postGcpTemplate(String name, String description, String gcpInstanceType, String gcpVolumeType = "HDD", String volumeCount, String volumeSize, Boolean publicInAccount) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "GCP", "NAME": name, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "GCP_INSTANCE_TYPE": gcpInstanceType, "GCP_VOLUME_TYPE": gcpVolumeType]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_GCP, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_GCP, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postOpenStackTemplate(String name, String description, String instanceType, String volumeCount, String volumeSize, Boolean publicInAccount) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "OPENSTACK", "NAME": name, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "INSTANCE_TYPE": instanceType]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_OPENSTACK, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_OPENSTACK, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postAzureTemplate(String name, String description, String instanceType, String volumeCount, String volumeSize, Boolean publicInAccount) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AZURE", "NAME": name, "DESCRIPTION": description, "INSTANCE_TYPE": instanceType, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_AZURE, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_AZURE, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def boolean hasAccess(String userId, String account, String ambariAddress) throws HttpResponseException {
        def stack = getStackByAmbari(ambariAddress)
        if (stack.owner == userId) {
            return true
        } else if (stack.public && stack.account == account) {
            return true
        }
    }

    def int resolveToStackId(String ambariAddress) throws HttpResponseException {
        getStackByAmbari(ambariAddress)?.id
    }

    def private getStackByAmbari(String ambariAddress) {
        def json = new JsonBuilder(["ambariAddress": ambariAddress]).toPrettyString()
        def context = createPostRequestContext(Resource.STACK_AMBARI.getPath(), ["json": json])
        doPost(context)?.data
    }

    def int putCluster(String ambari, String hostGroup, int scalingAdjustment, Boolean withStackUpdate = false) throws Exception {
        def stackId = getStackId(ambari)
        if (stackId) {
            putCluster(stackId, hostGroup, scalingAdjustment, withStackUpdate);
        }
        stackId
    }

    def void putCluster(int stackId, String hostGroup, int scalingAdjustment, Boolean withStackUpdate) throws Exception {
        log.debug("Putting cluster ...")
        def binding = ["HOST_GROUPS": new JsonBuilder(["hostGroup": hostGroup, "scalingAdjustment": scalingAdjustment, "withStackUpdate": withStackUpdate]).toPrettyString()]
        def json = createJson(Resource.CLUSTER_NODECOUNT_PUT.template(), binding)
        String path = Resource.CLUSTER_NODECOUNT_PUT.path().replaceFirst("stack-id", stackId.toString())
        def Map putCtx = createPostRequestContext(path, ['json': json])
        doPut(putCtx)
    }

    def void putClusterStatus(int stackId, String newStatus) throws Exception {
        log.debug("Putting cluster status...")
        def binding = ["NEW_STATUS": newStatus]
        def json = createJson(Resource.CLUSTER_STATUS_PUT.template(), binding)
        String path = Resource.CLUSTER_STATUS_PUT.path().replaceFirst("stack-id", stackId.toString())
        def Map putCtx = createPostRequestContext(path, ['json': json])
        doPut(putCtx)
    }

    def Object getFullStackStatus(int stackId) {
        String path = "${Resource.GLOBAL_STACKS.path()}/$stackId/status"
        Map getCtx = createGetRequestContext(path);
        def resp = doGet(getCtx)
        resp?.data
    }

    def String getStackStatus(int stackId) {
        getFullStackStatus(stackId)?.status
    }

    def byte[] getStackCertificate(Long stackId) {
        String path = "${Resource.GLOBAL_STACKS.path()}/$stackId/certificate"
        def context = createGetRequestContext(path)
        def resp = doGet(context)
        return resp.data.certificate.decodeBase64()
    }

    def void putStack(int stackId, String instanceGroup, int adjustment, Boolean withClusterUpdate = false) {
        def binding = ["INSTANCE_GROUP": instanceGroup, "ADJUSTMENT": adjustment, "WITHCLUSTEREVENT": withClusterUpdate]
        def json = createJson(Resource.GLOBAL_STACKS_NODECOUNT_PUT.template(), binding)
        String path = Resource.GLOBAL_STACKS_NODECOUNT_PUT.path() + "/$stackId"
        def Map putCtx = createPostRequestContext(path, ['json': json])
        doPut(putCtx)
    }

    def void putStackStatus(int stackId, String status) {
        def binding = ["NEW_STATUS": status]
        def json = createJson(Resource.GLOBAL_STACKS_STATUS_PUT.template(), binding)
        String path = Resource.GLOBAL_STACKS_STATUS_PUT.path() + "/$stackId"
        def Map putCtx = createPostRequestContext(path, ['json': json])
        doPut(putCtx)
    }

    def void postCluster(String name, String userName, String password, Integer blueprintId, String description, Integer stackId, List<Map<String, Object>> hostGroups) throws Exception {
        postCluster(name, userName, password, blueprintId, description, stackId, hostGroups, null, null, null, null, null, null, null, null)
    }

    def void postCluster(String name, String userName, String password, Integer blueprintId, String description, Integer stackId, List<Map<String, Object>> hostGroups,
                         Boolean enableSecurity, String kerberosMasterKey, String kerberosAdmin, String kerberosPassword) throws Exception {
        postCluster(name, userName, password, blueprintId, description, stackId, hostGroups, null, null, null, null, null, null, null, null, enableSecurity, kerberosMasterKey, kerberosAdmin, kerberosPassword)
    }

    def void postCluster(String name, String userName, String password, Integer blueprintId, String description, Integer stackId, List<Map<String, Object>> hostGroups,
                         String stack, String version, String os, String stackRepoId, String stackBaseURL,
                         String utilsRepoId, String utilsBaseURL, Boolean verify,
                         Boolean enableSecurity = false, String kerberosMasterKey = null, String kerberosAdmin = null, String kerberosPassword = null) throws Exception {
        postCluster(name, userName, password, blueprintId, description, stackId, hostGroups, stack, version, os, stackRepoId, stackBaseURL, utilsRepoId, utilsBaseURL, verify, enableSecurity, kerberosMasterKey, kerberosAdmin, kerberosPassword, null, null, null)
    }

    def void postCluster(String name, String userName, String password, Integer blueprintId, String description, Integer stackId, List<Map<String, Object>> hostGroups,
                         String stack, String version, String os, String stackRepoId, String stackBaseURL,
                         String utilsRepoId, String utilsBaseURL, Boolean verify,
                         Boolean enableSecurity, String kerberosMasterKey, String kerberosAdmin, String kerberosPassword,
                         String fileSytemType, Map<String, Object> properties, Boolean defaultFs) throws Exception {
        log.debug("Posting cluster ...")
        String hostGroupsJson = new JsonBuilder(hostGroups).toPrettyString();
        def stackDetails = null
        def fileSystem = null
        if (fileSytemType) {
            fileSystem = ["name"      : name,
                          "type"      : fileSytemType,
                          "defaultFs" : defaultFs,
                          "properties": properties
            ]
        }
        if (stack) {
            stackDetails = ["stack"       : stack,
                            "version"     : version,
                            "os"          : os,
                            "stackRepoId" : stackRepoId,
                            "stackBaseURL": stackBaseURL,
                            "utilsRepoId" : utilsRepoId,
                            "utilsBaseURL": utilsBaseURL,
                            "verify"      : verify
            ]
        }
        def binding = ["NAME"               : name,
                       "BLUEPRINT_ID"       : blueprintId,
                       "DESCRIPTION"        : description,
                       "HOSTGROUPS"         : hostGroupsJson,
                       "USERNAME"           : userName,
                       "PASSWORD"           : password,
                       "STACK_DETAILS"      : new JsonBuilder(stackDetails).toPrettyString(),
                       "FILESYSTEM"         : new JsonBuilder(fileSystem).toPrettyString(),
                       "ENABLE_SECURITY"    : enableSecurity,
                       "KERBEROS_MASTER_KEY": kerberosMasterKey ? "\"$kerberosMasterKey\"" : null,
                       "KERBEROS_ADMIN"     : kerberosAdmin ? "\"$kerberosAdmin\"" : null,
                       "KERBEROS_PASSWORD"  : kerberosPassword ? "\"$kerberosPassword\"" : null,
        ]
        def json = createJson(Resource.CLUSTERS.template(), binding)
        String path = Resource.CLUSTERS.path().replaceFirst("stack-id", stackId.toString())
        def Map postCtx = createPostRequestContext(path, ['json': json])
        doPost(postCtx)
    }

    def void addDefaultBlueprints() throws HttpResponseException {
        postBlueprint("multi-node-hdfs-yarn", "multi-node-hdfs-yarn", getResourceContent("blueprints/multi-node-hdfs-yarn"), false)
        postBlueprint("hdp-multinode-default", "hdp-multinode-default", getResourceContent("blueprints/hdp-multinode-default"), false)
    }

    def boolean health() throws Exception {
        log.debug("Checking health ...")
        Map getCtx = createGetRequestContext('health')
        Object healthObj = doGet(getCtx)
        return healthObj.data.status == 'ok'
    }

    def List<Map> getPrivateCredentials() throws Exception {
        log.debug("Getting credentials...")
        getAllAsList(Resource.USER_CREDENTIALS)
    }

    def Map<String, String> getPrivateCredentialsMap() throws Exception {
        def result = getPrivateCredentials()?.collectEntries {
            [(it.id as String): it.name + ":" + it.cloudPlatform]
        }
        result ?: new HashMap()
    }

    def List<Map> getAccountCredentials() throws Exception {
        log.debug("Getting credentials...")
        getAllAsList(Resource.ACCOUNT_CREDENTIALS)
    }

    def Map<String, String> getAccountCredentialsMap() throws Exception {
        def result = getAccountCredentials()?.collectEntries {
            [(it.id as String): it.name + ":" + it.cloudPlatform]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getCredentialMap(String id) throws Exception {
        def result = getCredential(id)?.collectEntries {
            if (it.key == "parameters") {
                it.value.collectEntries {
                    [(it.key as String): it.value as String]
                }
            } else {
                [(it.key as String): it.value as String]
            }
        }
        result ?: new HashMap()
    }

    def List<Map> getPrivateBlueprints() throws Exception {
        log.debug("Getting blueprints...")
        getAllAsList(Resource.USER_BLUEPRINTS)
    }

    def Map<String, String> getBlueprintsMap() throws Exception {
        def result = getPrivateBlueprints()?.collectEntries {
            [(it.id as String): it.name + ":" + it.blueprintName]
        }
        result ?: new HashMap()
    }

    def List<Map> getAccountBlueprints() throws Exception {
        log.debug("Getting blueprints...")
        getAllAsList(Resource.ACCOUNT_BLUEPRINTS)
    }

    def Map<String, String> getAccountBlueprintsMap() throws Exception {
        def result = getAccountBlueprints()?.collectEntries {
            [(it.id as String): it.name + ":" + it.blueprintName]
        }
        result ?: new HashMap()
    }

    def List<Map> getAccountRecipes() throws Exception {
        log.debug("Getting recipes...")
        getAllAsList(Resource.ACCOUNT_RECIPES)
    }

    def Map<String, String> getAccountRecipesMap() throws Exception {
        def result = getAccountRecipes()?.collectEntries {
            [(it.id as String): it.name]
        }
        result ?: new HashMap()
    }

    def List<Map> getPrivateNetworks() throws Exception {
        log.debug("Getting networks...")
        getAllAsList(Resource.USER_NETWORKS)
    }

    def List<Map> getAccountNetworks() throws Exception {
        log.debug("Getting networks...")
        getAllAsList(Resource.ACCOUNT_NETWORKS)
    }

    def Map<String, String> getAccountNetworksMap() throws Exception {
        def result = getAccountNetworks()?.collectEntries {
            [(it.id as String): it.name]
        }
        result ?: new HashMap()
    }

    def List<Map> getPrivateSecurityGroups() throws Exception {
        log.debug("Getting networks...")
        getAllAsList(Resource.USER_SECURITY_GROUPS)
    }

    def List<Map> getAccountSecurityGroups() throws Exception {
        log.debug("Getting security groups...")
        getAllAsList(Resource.ACCOUNT_SECURITY_GROUPS)
    }

    def Map<String, String> getAccountSecurityGroupsMap() throws Exception {
        def result = getAccountSecurityGroups()?.collectEntries {
            [(it.id as String): it.name]
        }
        result ?: new HashMap()
    }

    def Map<String, Object> getBlueprintMap(String id) throws Exception {
        def result = getBlueprint(id)?.collectEntries {
            if (it.key == "parameters") {
                it.value.collectEntries {
                    [(it.key as String): it.value as String]
                }
            } else if (it.key == "ambariBlueprint") {
                def result = it.value.host_groups?.collectEntries { [(it.name): it.components.collect { it.name }] }
                [(it.key as String): result as Map]
            } else {
                [(it.key as String): it.value as String]
            }
        }
        result ?: new HashMap()
    }

    def Map<String, Object> getRecipeMap(String id) throws Exception {
        def result = getRecipe(id)?.collectEntries {
            if (it.key == "plugins") {
                def result = it.value
                [(it.key as String): it.value as Map]
            } else if (it.key == "properties") {
                def result = it.value
                [(it.key as String): result as Map]
            } else {
                [(it.key as String): it.value as String]
            }
        }
        result ?: new HashMap()
    }

    def List<Map> getPrivateTemplates() throws Exception {
        log.debug("Getting templates...")
        getAllAsList(Resource.USER_TEMPLATES)
    }

    def Map<String, String> getPrivateTemplatesMap() throws Exception {
        def result = getPrivateTemplates()?.collectEntries {
            [(it.id as String): it.name + ":" + it.description]
        }
        result ?: new HashMap()
    }

    def List<Map> getAccountTemplates() throws Exception {
        log.debug("Getting templates...")
        getAllAsList(Resource.ACCOUNT_TEMPLATES)
    }

    def Map<String, String> getAccountTemplatesMap() throws Exception {
        def result = getAccountTemplates()?.collectEntries {
            [(it.id as String): it.name + ":" + it.description]
        }
        result ?: new HashMap()
    }

    def Map<String, Map<String, String>> getAccountTemplatesWithCloudPlatformMap(String cloudPlatform = "") throws Exception {
        def templates = getAccountTemplates()
        def result
        if (cloudPlatform == "") {
            result = templates?.collectEntries {
                Map<String, String> inside = new HashMap<>()
                inside.put(it.name as String, it.cloudPlatform as String)
                [(it.id as String): inside]
            }
        } else {
            result = new HashMap<>()
            for (Map map : templates) {
                if ((map.cloudPlatform as String) == cloudPlatform) {
                    Map<String, String> inside = new HashMap<>()
                    inside.put(map.name as String, map.cloudPlatform as String)
                    result.put((map.id as String), inside)
                }
            }
        }
        result ?: new HashMap()
    }

    def Map<String, String> getTemplateMap(String id) throws Exception {
        def result = getTemplate(id)?.collectEntries {
            if (it.key == "parameters") {
                it.value.collectEntries {
                    [(it.key as String): it.value as String]
                }
            } else {
                [(it.key as String): it.value as String]
            }
        }
        result ?: new HashMap()
    }

    def List<Map> getPrivateStacks() throws Exception {
        log.debug("Getting templates...")
        getAllAsList(Resource.USER_STACKS)
    }

    def Map<String, String> getPrivateStacksMap() throws Exception {
        def result = getPrivateStacks()?.collectEntries {
            [(it.id as String): it.name + ":" + it.nodeCount]
        }
        result ?: new HashMap()
    }

    def List<Map> getAccountStacks() throws Exception {
        log.debug("Getting templates...")
        getAllAsList(Resource.ACCOUNT_STACKS)
    }

    def Map<String, String> getAccountStacksMap() throws Exception {
        def result = getAccountStacks()?.collectEntries {
            [(it.id as String): it.name + ":" + it.nodeCount]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getStackMap(String id) throws Exception {
        def result = getStack(id)?.collectEntries {
            [(it.key as String): it.value as String]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getClusterMap(String id) throws Exception {
        def result = getCluster(id)?.collectEntries {
            if (it.key == "parameters") {
                it.value.collectEntries {
                    [(it.key as String): it.value as String]
                }
            } else {
                [(it.key as String): it.value as String]
            }
        }
        result ?: new HashMap()
    }

    def Object getStack(String id) throws Exception {
        log.debug("Getting stack...")
        return getOne(Resource.GLOBAL_STACKS, id)
    }

    def Object getStackByName(String name) throws Exception {
        log.debug("Getting stack...")
        return getOne(Resource.ACCOUNT_STACKS, name)
    }

    def Object deleteStack(String id) throws Exception {
        log.debug("Delete stack...")
        return deleteOne(Resource.GLOBAL_STACKS, id)
    }

    def Object deleteTemplate(String id) throws Exception {
        log.debug("Delete template...")
        return deleteOne(Resource.GLOBAL_TEMPLATES, id)
    }

    def Object deleteTemplateByName(String name) throws Exception {
        log.debug("Delete template...")
        return deleteOne(Resource.ACCOUNT_TEMPLATES, name)
    }

    def Object deleteCredential(String id) throws Exception {
        log.debug("Delete credential...")
        return deleteOne(Resource.GLOBAL_CREDENTIALS, id)
    }

    def Object deleteCredentialByName(String name) throws Exception {
        log.debug("Delete credential...")
        return deleteOne(Resource.ACCOUNT_CREDENTIALS, name)
    }

    def Object deleteBlueprint(String id) throws Exception {
        log.debug("Delete blueprint...")
        return deleteOne(Resource.GLOBAL_BLUEPRINTS, id)
    }

    def Object deleteBlueprintByName(String name) throws Exception {
        log.debug("Delete blueprint...")
        return deleteOne(Resource.ACCOUNT_BLUEPRINTS, name)
    }

    def Object deleteRecipe(String id) throws Exception {
        log.debug("Delete recipe...")
        return deleteOne(Resource.GLOBAL_RECIPES, id)
    }

    def Object deleteRecipeByName(String name) throws Exception {
        log.debug("Delete recipe...")
        return deleteOne(Resource.ACCOUNT_RECIPES, name)
    }

    def Object deleteNetwork(String id) throws Exception {
        log.debug("Delete network...")
        return deleteOne(Resource.GLOBAL_NETWORKS, id)
    }

    def Object deleteNetworkByName(String name) throws Exception {
        log.debug("Delete network...")
        return deleteOne(Resource.ACCOUNT_NETWORKS, name)
    }

    def Object deleteSecurityGroup(String id) throws Exception {
        log.debug("Delete security group...")
        return deleteOne(Resource.GLOBAL_SECURITY_GROUPS, id)
    }

    def Object deleteSecurityGroupByName(String name) throws Exception {
        log.debug("Delete security group...")
        return deleteOne(Resource.ACCOUNT_SECURITY_GROUPS, name)
    }

    def Object getCluster(String id) throws Exception {
        log.debug("Getting cluster...")
        String path = Resource.CLUSTERS.path().replaceFirst("stack-id", id.toString())
        return getOne(path)
    }

    def Object getCredential(String id) throws Exception {
        log.debug("Getting credential...")
        return getOne(Resource.GLOBAL_CREDENTIALS, id)
    }

    def Object getCredentialByName(String name) throws Exception {
        log.debug("Getting credential by name...")
        return getOne(Resource.ACCOUNT_CREDENTIALS, name)
    }

    def Object getTemplate(String id) throws Exception {
        log.debug("Getting template...")
        return getOne(Resource.GLOBAL_TEMPLATES, id)
    }

    def Object getTemplateByName(String name) throws Exception {
        log.debug("Getting template by name...")
        return getOne(Resource.ACCOUNT_TEMPLATES, name)
    }

    def Object getBlueprint(String id) throws Exception {
        log.debug("Getting blueprint...")
        return getOne(Resource.GLOBAL_BLUEPRINTS, id)
    }

    def Object getBlueprintByName(String name) throws Exception {
        log.debug("Getting blueprint by name...")
        return getOne(Resource.ACCOUNT_BLUEPRINTS, name)
    }

    def Object getRecipe(String id) throws Exception {
        log.debug("Getting recipe...")
        return getOne(Resource.GLOBAL_RECIPES, id)
    }

    def Object getRecipeByName(String name) throws Exception {
        log.debug("Getting recipe by name...")
        return getOne(Resource.ACCOUNT_RECIPES, name)
    }

    def Object terminateStack(String id) throws Exception {
        log.debug("Terminate stack...")
        return deleteOne(Resource.GLOBAL_STACKS, id)
    }

    def Object getNetwork(String id) throws Exception {
        log.debug("Getting network...")
        return getOne(Resource.GLOBAL_NETWORKS, id)
    }

    def Object getNetworkByName(String name) throws Exception {
        log.debug("Getting network by name...")
        return getOne(Resource.ACCOUNT_NETWORKS, name)
    }

    def Object getSecurityGroup(String id) throws Exception {
        log.debug("Getting security group by id...")
        return getOne(Resource.GLOBAL_SECURITY_GROUPS, id)
    }

    def Object getSecurityGroupByName(String name) throws Exception {
        log.debug("Getting security group by name...")
        return getOne(Resource.ACCOUNT_SECURITY_GROUPS, name)
    }

    def String postAzureNetwork(String name, String description, String subnetCIDR, String addressPrefixCIDR, Boolean publicInAccount) throws Exception {
        log.debug("Posting Azure network ...")
        def binding = ["CLOUD_PLATFORM": "AZURE", "NAME": name, "DESCRIPTION": description, "SUBNET_CIDR": subnetCIDR, "ADDRESS_PREFIX_CIDR": addressPrefixCIDR]
        return postNetwork(Resource.ACCOUNT_NETWORKS_AZURE, Resource.USER_NETWORKS_AZURE, binding, publicInAccount)
    }

    def String postAWSNetwork(String name, String description, String subnetCIDR, String vpcId, String internetGatewayId, Boolean publicInAccount) throws Exception {
        log.debug("Posting AWS network ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "DESCRIPTION": description, "SUBNET_CIDR": subnetCIDR, "VPC_ID": vpcId, "INTERNET_GATEWAY_ID": internetGatewayId]
        return postNetwork(Resource.ACCOUNT_NETWORKS_AWS, Resource.USER_NETWORKS_AWS, binding, publicInAccount)
    }

    def String postGCPNetwork(String name, String description, String subnetCIDR, Boolean publicInAccount) throws Exception {
        log.debug("Posting GCP network ...")
        def binding = ["CLOUD_PLATFORM": "GCP", "NAME": name, "DESCRIPTION": description, "SUBNET_CIDR": subnetCIDR]
        return postNetwork(Resource.ACCOUNT_NETWORKS_GCP, Resource.USER_NETWORKS_GCP, binding, publicInAccount)
    }

    def String postOpenStackNetwork(String name, String description, String subnetCIDR, String publicNetId, Boolean publicInAccount) throws Exception {
        log.debug("Posting OpenStack network ...")
        def binding = ["CLOUD_PLATFORM": "OPENSTACK", "NAME": name, "DESCRIPTION": description, "SUBNET_CIDR": subnetCIDR, "PUBLIC_NET_ID": publicNetId]
        return postNetwork(Resource.ACCOUNT_NETWORKS_OPENSTACK, Resource.USER_NETWORKS_OPENSTACK, binding, publicInAccount)
    }

    def Object getPlatformVariants() {
        Map getCtx = createGetRequestContext(Resource.GLOBAL_STACKS_PLATFORM_VARIANT.path())
        return doGet(getCtx)?.data
    }

    def Object getCloudConnectorParams() {
      Map getCtx = createGetRequestContext(Resource.GLOBAL_CONNECTOR_PARAMS.path())
      return doGet(getCtx)?.data
    }

    def private String postNetwork(Resource accountResource, Resource userResource, Map binding, Boolean publicInAccount) {
        def response;
        if (publicInAccount) {
            response = processPost(accountResource, binding)
        } else {
            response = processPost(userResource, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postSecurityGroup(String name, String description, Map<String, String> tcpRules, Map<String, String> udpRules, Boolean publicInAccount) throws Exception {
        def tcp = tcpRules.collect { ["protocol": "tcp", "subnet": it.key, "ports": it.value] }
        def udp = udpRules.collect { ["protocol": "udp", "subnet": it.key, "ports": it.value] }
        def securityRules = tcp + udp
        def securityGroup = [
                "name"         : name,
                "description"  : description,
                "securityRules": securityRules
        ]
        def resource = publicInAccount ? Resource.ACCOUNT_SECURITY_GROUPS : Resource.USER_SECURITY_GROUPS
        def postCtx = createPostRequestContext(resource.path(), ['json': new JsonBuilder(securityGroup).toPrettyString()])
        def response = doPost(postCtx)
        return response?.data?.id
    }

    def private List getAllAsList(Resource resource) throws Exception {
        Map getCtx = createGetRequestContext(resource.path());
        Object response = doGet(getCtx);
        return response?.data
    }

    def private Object getOne(Resource resource, String id) throws Exception {
        String path = resource.path() + "/$id"
        Map getCtx = createGetRequestContext(path);
        Object response = doGet(getCtx)
        return response?.data
    }

    def private Object getOne(String resource) throws Exception {
        String path = resource
        Map getCtx = createGetRequestContext(path);
        Object response = doGet(getCtx)
        return response?.data
    }

    def private Object deleteOne(Resource resource, String id) throws Exception {
        String path = resource.path() + "/$id"
        Map getCtx = createGetRequestContext(path);
        Object response = doDelete(getCtx)
        return response?.data
    }

    def private Object doGet(Map getCtx) throws Exception {
        restClient.get(getCtx)
    }

    def private Object doDelete(Map getCtx) throws Exception {
        Object response = null;
        response = restClient.delete(getCtx)
        return response;
    }

    def private Object doPost(Map postCtx) throws Exception {
        Object response = null;
        try {
            response = restClient.post(postCtx)
        } catch (e) {
            log.error("ERROR: {}", e)
            throw e;
        }
        return response;
    }

    def private Object doPut(Map putCtx) throws Exception {
        Object response = null;
        try {
            response = restClient.put(putCtx)
        } catch (e) {
            log.error("ERROR: {}", e)
            throw e;
        }
        return response;
    }

    def private Map createGetRequestContext(String resourcePath) throws Exception {
        return ["path": "${restClient.uri}$resourcePath"]
    }

    def private Map createPostRequestContext(String resourcePath, Map ctx) throws Exception {
        def Map<String, ?> putRequestMap = [:]
        def String uri = "${restClient.uri}$resourcePath"
        putRequestMap.put('path', uri)
        putRequestMap.put('body', ctx.get("json"));
        putRequestMap.put('requestContentType', ContentType.JSON)
        return putRequestMap
    }

    def private String createJson(String templateName, Map bindings) throws Exception {
        def InputStream inPut = this.getClass().getClassLoader().getResourceAsStream("templates/${templateName}");
        String json = engine.createTemplate(new InputStreamReader(inPut)).make(bindings);
        return json;
    }

    def private Object processPost(Resource resource, Map binding) throws Exception {
        def json = createJson(resource.template(), binding)
        def Map postCtx = createPostRequestContext(resource.path(), ['json': json])
        return doPost(postCtx)
    }

    private String getResourceContent(name) {
        getClass().getClassLoader().getResourceAsStream(name)?.text
    }

    private int getStackId(String ambari) {
        getPrivateStacks().find { it.ambariServerIp.equals(ambari) }?.id
    }
}
