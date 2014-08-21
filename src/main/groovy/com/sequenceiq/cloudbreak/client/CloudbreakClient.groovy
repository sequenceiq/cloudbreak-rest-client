package com.sequenceiq.cloudbreak.client

import groovy.json.JsonSlurper
import groovy.text.SimpleTemplateEngine
import groovy.text.TemplateEngine
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient

@Slf4j
class CloudbreakClient {

    def private enum Resource {
        CREDENTIALS_EC2("credentials", "credentials_ec2.json"),
        CREDENTIALS_AZURE("credentials", "credentials_azure.json"),
        CREDENTIALS("credentials", "credentials.json"),
        TEMPLATES("templates", "template.json"),
        TEMPLATES_EC2("templates", "template_ec2.json"),
        SPOT_TEMPLATES_EC2("templates", "template_spot_ec2.json"),
        TEMPLATES_AZURE("templates", "template_azure.json"),
        STACKS("stacks", "stack.json"),
        STACK_NODECOUNT_PUT("stacks", "stack_nodecount_put.json"),
        CLUSTER_NODECOUNT_PUT("stacks/stack-id/cluster", "cluster_nodecount_put.json"),
        BLUEPRINTS("blueprints", "blueprint.json"),
        CLUSTERS("stacks/stack-id/cluster", "cluster.json"),
        CERTIFICATES("credentials/certificate", "certificate.json"),
        ME("me", "me.json")

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
    def slurper = new JsonSlurper()


    CloudbreakClient(host = 'localhost', port = '8080', user = 'cbuser@sequenceiq.com', password = 'test123') {
        restClient = new RESTClient("http://${host}:${port}/" as String)
        restClient.headers['Authorization'] = 'Basic ' + "$user:$password".getBytes('iso-8859-1').encodeBase64()
    }

    def String postStack(String stackName, String nodeCount, String credentialId, String templateId) throws Exception {
        log.debug("Posting stack ...")
        def binding = ["NODE_COUNT": nodeCount, "STACK_NAME": stackName, "CREDENTIAL_ID": credentialId, "TEMPLATE_ID": templateId]
        def response = processPost(Resource.STACKS, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postBlueprint(String name, String description, String blueprint) throws Exception {
        log.debug("Posting blueprint ...")
        def binding = ["BLUEPRINT": blueprint, "NAME": name, "DESCRIPTION": description]
        def response = processPost(Resource.BLUEPRINTS, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postEc2Credential(String name, String description, String roleArn, String sshKey) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "ROLE_ARN": roleArn, "DESCRIPTION": description, "SSHKEY": sshKey]
        def response  = processPost(Resource.CREDENTIALS_EC2, binding)
        return response?.data?.id
    }

    def String postAzureCredential(String name, String description, String subscriptionId, String jksPassword, String sshKey) throws Exception {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "AZURE", "NAME": name, "DESCRIPTION": description,"SUBSCRIPTIONID": subscriptionId, "JKSPASSWORD": jksPassword, "SSHKEY": sshKey]
        def response = processPost(Resource.CREDENTIALS_AZURE, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String getCertificate(String id) throws Exception {
        return getOne(Resource.CERTIFICATES, id).text
    }

    def String postSpotEc2Template(String name, String description, String region, String amiId, String sshLocation, String instanceType, String volumeCount, String volumeSize, String volumeType, String spotPrice) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "REGION": region, "AMI": amiId, "SSH_LOCATION": sshLocation, "INSTANCE_TYPE": instanceType, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "VOLUME_TYPE": volumeType, "SPOT_PRICE": spotPrice]
        def response = processPost(Resource.SPOT_TEMPLATES_EC2, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postEc2Template(String name, String description, String region, String amiId, String sshLocation, String instanceType, String volumeCount, String volumeSize, String volumeType) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "REGION": region, "AMI": amiId, "SSH_LOCATION": sshLocation, "INSTANCE_TYPE": instanceType, "DESCRIPTION": description, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize, "VOLUME_TYPE": volumeType]
        def response = processPost(Resource.TEMPLATES_EC2, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postAzureTemplate(String name, String description, String region, String instanceName, String instanceType, String volumeCount, String volumeSize) throws Exception {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AZURE", "NAME": name, "DESCRIPTION": description, "IMAGE_NAME": instanceName, "REGION": region, "INSTANCE_TYPE": instanceType, "VOLUME_COUNT": volumeCount, "VOLUME_SIZE": volumeSize]
        def response = processPost(Resource.TEMPLATES_AZURE, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def void putCluster(String stackId, Map<String, Integer> hostgroupAssociations) throws Exception {
        log.debug("Puting cluster ...")
        StringBuilder hosts = new StringBuilder()
        for (Map.Entry<String, Integer> entry : hostgroupAssociations.entrySet()) {
            hosts.append(String.format("{\"%s\":%d},", entry.getKey(), entry.getValue()))
        }
        def binding = ["HOSTS": hosts.toString().substring(0, hosts.toString().length()-1)]
        def json = createJson(Resource.CLUSTER_NODECOUNT_PUT.template(), binding)
        String path = Resource.CLUSTER_NODECOUNT_PUT.path().replaceFirst("stack-id", stackId.toString())
        def Map putCtx = createPostRequestContext(path, ['json': json])
        def response = doPut(putCtx)
    }

    def void putStack(String id, Integer nodeCount) throws Exception {
        log.debug("Puting stack ...")
        def binding = ["NODE_COUNT": nodeCount]
        def json = createJson(Resource.STACK_NODECOUNT_PUT.template(), binding)
        String path = Resource.STACK_NODECOUNT_PUT.path() + "/$id"
        def Map putCtx = createPostRequestContext(path, ['json': json])
        def response = doPut(putCtx)
    }

    def void postCluster(String clusterName, Integer blueprintId, String descrition, Integer stackId) throws Exception {
        log.debug("Posting cluster ...")
        def binding = ["CLUSTER_NAME": clusterName, "BLUEPRINT_ID": blueprintId, "DESCRIPTION": descrition]
        def json = createJson(Resource.CLUSTERS.template(), binding)
        String path = Resource.CLUSTERS.path().replaceFirst("stack-id", stackId.toString())
        def Map postCtx = createPostRequestContext(path, ['json': json])
        def response = doPost(postCtx)
    }

    def void addDefaultBlueprints() throws HttpResponseException {
        postBlueprint("multi-node-hdfs-yarn", "multi-node-hdfs-yarn", getResourceContent("blueprints/multi-node-hdfs-yarn"))
        postBlueprint("single-node-hdfs-yarn", "single-node-hdfs-yarn", getResourceContent("blueprints/single-node-hdfs-yarn"))
        postBlueprint("lambda-architecture", "lambda-architecture", getResourceContent("blueprints/lambda-architecture"))
        postBlueprint("warmup", "warmup", getResourceContent("blueprints/warmup"))
    }

    def boolean health() throws Exception {
        log.debug("Checking health ...")
        Map getCtx = createGetRequestContext('health', null)
        Object healthObj = doGet(getCtx)
        return healthObj.data.status == 'ok'
    }

    def boolean login() throws Exception {
        log.debug("Getting login...")
        return getNothing(Resource.ME).email != null
    }

    def List<Map> getCredentials() throws Exception {
        log.debug("Getting credentials...")
        getAllAsList(Resource.CREDENTIALS)
    }

    def Map<String, String> getCredentialsMap() throws Exception {
        def result = getCredentials()?.collectEntries {
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

    def List<Map> getBlueprints() throws Exception {
        log.debug("Getting blueprints...")
        getAllAsList(Resource.BLUEPRINTS)
    }

    def Map<String, String> getBlueprintsMap() throws Exception {
        def result = getBlueprints()?.collectEntries {
            [(it.id as String): it.name + ":" + it.blueprintName]
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

    def Map<String, String> getTemplatesMap() throws Exception {
        def result = getTemplates()?.collectEntries {
            [(it.id as String): it.name + ":" + it.description]
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

    def Map<String, String> getStacksMap() throws Exception {
        def result = getStacks()?.collectEntries {
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

    def Map<String, String> getClustersMap() throws Exception {
        def result = getClusters()?.collectEntries {
            [(it.id as String): it.cluster + ":" + it.status]
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

    def List<Map> getTemplates() throws Exception {
        log.debug("Getting templates...")
        getAllAsList(Resource.TEMPLATES)
    }

    def List<Map> getStacks() throws Exception {
        log.debug("Getting templates...")
        getAllAsList(Resource.STACKS)
    }

    def List<Map> getClusters() throws Exception {
        log.debug("Getting clusters...")
        getAllAsList(Resource.CLUSTERS)
    }

    def Object getStack(String id) throws Exception {
        log.debug("Getting stack...")
        return getOne(Resource.STACKS, id)
    }

    def Object deleteStack(String id) throws Exception {
        log.debug("Delete stack...")
        return deleteOne(Resource.STACKS, id)
    }

    def Object deleteTemplate(String id) throws Exception {
        log.debug("Delete template...")
        return deleteOne(Resource.TEMPLATES, id)
    }

    def Object deleteCredential(String id) throws Exception {
        log.debug("Delete credential...")
        return deleteOne(Resource.CREDENTIALS, id)
    }

    def Object deleteBlueprint(String id) throws Exception {
        log.debug("Delete blueprint...")
        return deleteOne(Resource.BLUEPRINTS, id)
    }

    def Object getCluster(String id) throws Exception {
        log.debug("Getting cluster...")
        String path = Resource.CLUSTERS.path().replaceFirst("stack-id", id.toString())
        return getOne(path)
    }

    def Object getCredential(String id) throws Exception {
        log.debug("Getting credentials...")
        return getOne(Resource.CREDENTIALS, id)
    }

    def Object getTemplate(String id) throws Exception {
        log.debug("Getting credentials...")
        return getOne(Resource.TEMPLATES, id)
    }

    def Object getBlueprint(String id) throws Exception {
        log.debug("Getting credentials...")
        return getOne(Resource.BLUEPRINTS, id)
    }

    def Object terminateStack(String id) throws Exception {
        log.debug("Terminate stack...")
        return deleteOne(Resource.STACKS, id)
    }

    def private List getAllAsList(Resource resource) throws Exception {
        Map getCtx = createGetRequestContext(resource.path(), [:]);
        Object response = doGet(getCtx);
        return response?.data
    }

    def private Object getOne(Resource resource, String id) throws Exception {
        String path = resource.path() + "/$id"
        Map getCtx = createGetRequestContext(path, [:]);
        Object response = doGet(getCtx)
        return response?.data
    }

    def private Object getNothing(Resource resource) throws Exception {
        String path = resource.path()
        Map getCtx = createGetRequestContext(path, [:]);
        Object response = doGet(getCtx)
        return response?.data
    }

    def private Object getOne(String resource) throws Exception {
        String path = resource
        Map getCtx = createGetRequestContext(path, [:]);
        Object response = doGet(getCtx)
        return response?.data
    }

    def private Object deleteOne(Resource resource, String id) throws Exception {
        String path = resource.path() + "/$id"
        Map getCtx = createGetRequestContext(path, [:]);
        Object response = doDelete(getCtx)
        return response?.data
    }

    def private Object doGet(Map getCtx) throws Exception {
        Object response = null;
        response = restClient.get(getCtx)
        return response;
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
            throw  e;
        }
        return response;
    }

    def private Object doPut(Map putCtx) throws Exception {
        Object response = null;
        try {
            response = restClient.put(putCtx)
        } catch (e) {
            log.error("ERROR: {}", e)
            throw  e;
        }
        return response;
    }

    def private Map createGetRequestContext(String resourcePath, Map ctx) throws Exception {
        Map context = [:]
        String uri = "${restClient.uri}$resourcePath"
        context.put('path', uri)
        return context
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
}

