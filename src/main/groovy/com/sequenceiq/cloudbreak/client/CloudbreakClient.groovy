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
        USER_CREDENTIALS_GCC("user/credentials", "credentials_gcc.json"),
        ACCOUNT_CREDENTIALS("account/credentials", "credentials.json"),
        ACCOUNT_CREDENTIALS_EC2("account/credentials", "credentials_ec2.json"),
        ACCOUNT_CREDENTIALS_AZURE("account/credentials", "credentials_azure.json"),
        ACCOUNT_CREDENTIALS_GCC("account/credentials", "credentials_gcc.json"),
        GLOBAL_CREDENTIALS("credentials", ""),
        USER_TEMPLATES("user/templates", "template.json"),
        USER_TEMPLATES_EC2("user/templates", "template_ec2.json"),
        USER_TEMPLATES_EC2_SPOT("user/templates", "template_spot_ec2.json"),
        USER_TEMPLATES_GCC("user/templates", "template_gcc.json"),
        USER_TEMPLATES_AZURE("user/templates", "template_azure.json"),
        ACCOUNT_TEMPLATES("account/templates", "template.json"),
        ACCOUNT_TEMPLATES_EC2("account/templates", "template_ec2.json"),
        ACCOUNT_TEMPLATES_EC2_SPOT("account/templates", "template_spot_ec2.json"),
        ACCOUNT_TEMPLATES_AZURE("account/templates", "template_azure.json"),
        ACCOUNT_TEMPLATES_GCC("account/templates", "template_gcc.json"),
        GLOBAL_TEMPLATES("templates", ""),
        USER_STACKS("user/stacks", "stack.json"),
        ACCOUNT_STACKS("account/stacks", "stack.json"),
        ACCOUNT_STACKS_WITH_IMAGE("account/stacks", "stackimage.json"),
        USER_STACKS_WITH_IMAGE("user/stacks", "stackimage.json"),
        GLOBAL_STACKS_NODECOUNT_PUT("stacks", "stack_nodecount_put.json"),
        GLOBAL_STACKS("stacks", ""),
        STACK_AMBARI("stacks/ambari", ""),
        USER_BLUEPRINTS("user/blueprints", "blueprint.json"),
        ACCOUNT_BLUEPRINTS("account/blueprints", "blueprint.json"),
        GLOBAL_BLUEPRINTS("blueprints", "blueprint.json"),
        CLUSTER_NODECOUNT_PUT("stacks/stack-id/cluster", "cluster_nodecount_put.json"),
        CLUSTERS("stacks/stack-id/cluster", "cluster.json"),
        CERTIFICATES("credentials/certificate", "certificate.json"),
        ACCOUNT_RECIPES("account/recipes", "recipe.json"),
        USER_RECIPES("user/recipes", "recipe.json"),
        GLOBAL_RECIPES("recipes", "")

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
        restClient.ignoreSSLIssues();
        restClient.headers['Authorization'] = 'Bearer ' + token
    }

    def String postStack(String stackName, String userName, String password, String credentialId, String region, Boolean publicInAccount, Map<String, Map<Long, Integer>> hostGroupTemplates, String onFailure, Long threshold, String adjustmentType, String image = null) throws Exception {
        log.debug("Posting stack ...")
        StringBuilder group = new StringBuilder();
        for (Map.Entry<String, Map<Long, Integer>> map: hostGroupTemplates.entrySet()) {
            for (Map.Entry<Long, Integer> entry : map.value.entrySet()) {
                group.append("{");
                group.append(String.format("\"templateId\": %s, \"group\": \"%s\", \"nodeCount\": %s", entry.getKey(), map.getKey(), entry.getValue()));
                group.append("},");
            }
        }
        def response;
        if (image == null || image == "") {
            def binding = ["STACK_NAME"   : stackName,
                           "CREDENTIAL_ID": credentialId,
                           "REGION"       : region,
                           "USER_NAME"    : userName,
                           "PASSWORD"     : password,
                           "ON_FAILURE"   : onFailure,
                           "THRESHOLD"    : threshold,
                           "ADJUSTMENTTYPE" : adjustmentType,
                           "GROUPS"       : group.toString().substring(0, group.toString().length() - 1)]
            if (publicInAccount) {
                response = processPost(Resource.ACCOUNT_STACKS, binding)
            } else {
                response = processPost(Resource.USER_STACKS, binding)
            }
        } else {
            def binding = ["STACK_NAME"   : stackName,
                           "CREDENTIAL_ID": credentialId,
                           "REGION"       : region,
                           "USER_NAME"    : userName,
                           "IMAGE"        : image,
                           "PASSWORD"     : password,
                           "ON_FAILURE"   : onFailure,
                           "THRESHOLD"    : threshold,
                           "ADJUSTMENTTYPE" : adjustmentType,
                           "GROUPS"       : group.toString().substring(0, group.toString().length() - 1)]
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

    def String postGccCredential(String name, String description, String sshKey, Boolean publicInAccount, String projectId, String serviceAccountId, String serviceAccountPrivateKey) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "GCC", "NAME": name, "PROJECT_ID": projectId, "DESCRIPTION": description, "SSHKEY": sshKey, "SERVICE_ACCOUNT_ID": serviceAccountId, "SERVICE_ACCOUNT_PRIVATE_KEY": serviceAccountPrivateKey]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_CREDENTIALS_GCC, binding)
        } else {
            response = processPost(Resource.USER_CREDENTIALS_GCC, binding)
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

    def String getCertificate(String id) throws Exception {
        return getOne(Resource.CERTIFICATE, id).text
    }

    def String postSpotEc2Template(String name, String description, String sshLocation, String instanceType, String volumeCount, String volumeSize, String volumeType, String spotPrice, Boolean publicInAccount) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "SSH_LOCATION": sshLocation, "INSTANCE_TYPE": instanceType, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "VOLUME_TYPE": volumeType, "SPOT_PRICE": spotPrice]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_EC2_SPOT, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_EC2_SPOT, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postEc2Template(String name, String description, String sshLocation, String instanceType, String volumeCount, String volumeSize, String volumeType, Boolean publicInAccount) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "SSH_LOCATION": sshLocation, "INSTANCE_TYPE": instanceType, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "VOLUME_TYPE": volumeType]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_EC2, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_EC2, binding)
        }
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postGccTemplate(String name, String description, String gccInstanceType, String volumeCount, String volumeSize, Boolean publicInAccount) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "GCC", "NAME": name, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "GCC_INSTANCE_TYPE": gccInstanceType]
        def response;
        if (publicInAccount) {
            response = processPost(Resource.ACCOUNT_TEMPLATES_GCC, binding)
        } else {
            response = processPost(Resource.USER_TEMPLATES_GCC, binding)
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

    def int putCluster(String ambari, String hostGroup, int scalingAdjustment) throws Exception {
        def stackId = getStackId(ambari)
        if (stackId) {
            putCluster(stackId, hostGroup, scalingAdjustment);
        }
        stackId
    }

    def void putCluster(int stackId, String hostGroup, int scalingAdjustment) throws Exception {
        log.debug("Putting cluster ...")
        def binding = ["HOST_GROUPS": new JsonBuilder(["hostGroup": hostGroup, "scalingAdjustment": scalingAdjustment]).toPrettyString()]
        def json = createJson(Resource.CLUSTER_NODECOUNT_PUT.template(), binding)
        String path = Resource.CLUSTER_NODECOUNT_PUT.path().replaceFirst("stack-id", stackId.toString())
        def Map putCtx = createPostRequestContext(path, ['json': json])
        doPut(putCtx)
    }

    def String getStackStatus(int stackId) {
        String path = "${Resource.GLOBAL_STACKS.path()}/$stackId/status"
        Map getCtx = createGetRequestContext(path);
        def resp = doGet(getCtx)
        resp?.data?.status
    }

    def void putStack(int stackId, String hostGroup, int adjustment) {
        def binding = ["HOST_GROUP": hostGroup, "ADJUSTMENT": adjustment]
        def json = createJson(Resource.GLOBAL_STACKS_NODECOUNT_PUT.template(), binding)
        String path = Resource.GLOBAL_STACKS_NODECOUNT_PUT.path() + "/$stackId"
        def Map putCtx = createPostRequestContext(path, ['json': json])
        doPut(putCtx)
    }

    def void postCluster(String name, Integer blueprintId, Integer recipeId, String description, Integer stackId) throws Exception {
        log.debug("Posting cluster ...")
        def binding = ["NAME": name, "BLUEPRINT_ID": blueprintId, "RECIPE_ID": recipeId, "DESCRIPTION": description]
        def json = createJson(Resource.CLUSTERS.template(), binding)
        String path = Resource.CLUSTERS.path().replaceFirst("stack-id", stackId.toString())
        def Map postCtx = createPostRequestContext(path, ['json': json])
        def response = doPost(postCtx)
    }

    def void addDefaultBlueprints() throws HttpResponseException {
        postBlueprint("multi-node-hdfs-yarn", "multi-node-hdfs-yarn", getResourceContent("blueprints/multi-node-hdfs-yarn"), false)
        postBlueprint("hdp-multinode-default", "hdp-multinode-default", getResourceContent("blueprints/hdp-multinode-default"), false)
        postBlueprint("lambda-architecture", "lambda-architecture", getResourceContent("blueprints/lambda-architecture"), false)
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
                [(it.key as String): it.value as List]
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
        if(cloudPlatform == "") {
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

    def Object deleteStack(String id) throws Exception {
        log.debug("Delete stack...")
        return deleteOne(Resource.GLOBAL_STACKS, id)
    }

    def Object deleteTemplate(String id) throws Exception {
        log.debug("Delete template...")
        return deleteOne(Resource.GLOBAL_TEMPLATES, id)
    }

    def Object deleteCredential(String id) throws Exception {
        log.debug("Delete credential...")
        return deleteOne(Resource.GLOBAL_CREDENTIALS, id)
    }

    def Object deleteBlueprint(String id) throws Exception {
        log.debug("Delete blueprint...")
        return deleteOne(Resource.GLOBAL_BLUEPRINTS, id)
    }

    def Object deleteRecipe(String id) throws Exception {
        log.debug("Delete recipe...")
        return deleteOne(Resource.GLOBAL_RECIPES, id)
    }

    def Object getCluster(String id) throws Exception {
        log.debug("Getting cluster...")
        String path = Resource.CLUSTERS.path().replaceFirst("stack-id", id.toString())
        return getOne(path)
    }

    def Object getCredential(String id) throws Exception {
        log.debug("Getting credentials...")
        return getOne(Resource.GLOBAL_CREDENTIALS, id)
    }

    def Object getTemplate(String id) throws Exception {
        log.debug("Getting credentials...")
        return getOne(Resource.GLOBAL_TEMPLATES, id)
    }

    def Object getBlueprint(String id) throws Exception {
        log.debug("Getting blueprint...")
        return getOne(Resource.GLOBAL_BLUEPRINTS, id)
    }

    def Object getRecipe(String id) throws Exception {
        log.debug("Getting recipe...")
        return getOne(Resource.GLOBAL_RECIPES, id)
    }

    def Object terminateStack(String id) throws Exception {
        log.debug("Terminate stack...")
        return deleteOne(Resource.GLOBAL_STACKS, id)
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
