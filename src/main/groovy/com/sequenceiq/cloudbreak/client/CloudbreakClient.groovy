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
        CREDENTIALS("credential", "credentials.json"),
        TEMPLATES("template", "template.json"),
        STACKS("stack", "stack.json"),
        BLUEPRINTS("blueprint", "blueprint.json"),
        CLUSTERS("stack/stack-id/cluster", "cluster.json")
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


    CloudbreakClient(host = 'localhost', port = '8080', user = 'user@seq.com', password = 'test123') {
        restClient = new RESTClient("http://${host}:${port}/" as String)
        restClient.headers['Authorization'] = 'Basic ' + "$user:$password".getBytes('iso-8859-1').encodeBase64()
    }

    def String postCredentials() {
        log.debug("Posting credentials ...")
        def binding = [:]
        def response = processPost(Resource.CREDENTIALS, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postTemplate(String name) {
        log.debug("Posting template ...")
        def binding = ["NAME": "$name"]
        def response = processPost(Resource.TEMPLATES, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postStack(String stackName, String nodeCount, String credentialId, String templateId) {
        log.debug("Posting stack ...")
        def binding = ["NODE_COUNT": nodeCount, "STACK_NAME": stackName, "CREDENTIAL_ID": credentialId, "TEMPLATE_ID": templateId]
        def response = processPost(Resource.STACKS, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postBlueprint(String name, String description, String blueprint) {
        log.debug("Posting blueprint ...")
        def binding = ["BLUEPRINT": blueprint, "NAME": name, "DESCRIPTION": description]
        def response = processPost(Resource.BLUEPRINTS, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postEc2Credential(String name, String description, String roleArn, String instanceProfileRoleArn) {
        log.debug("Posting credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "DESCRIPTION": description, "ROLE_ARN": roleArn, "INSTANCE_PROFILE_ROLE_ARN": instanceProfileRoleArn]
        def response = processPost(Resource.CREDENTIALS, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def String postEc2Template(String name, String description, String region, String amiId, String keyName, String sshLocation, String instanceType) {
        log.debug("testing credential ...")
        def binding = ["CLOUD_PLATFORM": "AWS", "NAME": name, "DESCRIPTION": description, "REGION": region, "AMI": amiId, "KEYNAME": keyName, "SSH_LOCATION": sshLocation, "INSTANCE_TYPE": instanceType]
        def response = processPost(Resource.TEMPLATES, binding)
        log.debug("Got response: {}", response.data.id)
        return response?.data?.id
    }

    def void postCluster(String clusterName, Integer blueprintId, String descrition, Integer stackId) {
        log.debug("Posting cluster ...")
        def binding = ["CLUSTER_NAME": clusterName, "BLUEPRINT_ID": blueprintId, "DESCRIPTION": descition]
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

    def void addDefaultCredentials() throws HttpResponseException {
        postEc2Credential("default aws",
                "my default aws credential",
                "arn:aws:iam::******:role/seq-self-cf",
                "arn:aws:iam::****:instance-profile/readonly-role"
        )
    }

    def void addDefaultTemplates() throws HttpResponseException {
        postEc2Template("DefaultAwsTemplate",
                "my default aws template",
                "EU_WEST_1",
                "ami-f39f5684",
                "sequence-eu",
                "0.0.0.0/0",
                "M1Small"
        )
    }

    def boolean health() {
        log.debug("Checking health ...")
        Map getCtx = createGetRequestContext('health', null)
        Object healthObj = doGet(getCtx)
        return healthObj.data.status == 'ok'
    }

    def List<Map> getCredentials() {
        log.debug("Getting credentials...")
        getAllAsList(Resource.CREDENTIALS)
    }

    def Map<String, String> getCredentialsMap() {
        def result = getCredentials()?.collectEntries {
            [(it.id as String): it.name + ":" + it.cloudPlatform]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getCredentialMap(String id) {
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

    def List<Map> getBlueprints() {
        log.debug("Getting blueprints...")
        getAllAsList(Resource.BLUEPRINTS)
    }

    def Map<String, String> getBlueprintsMap() {
        def result = getBlueprints()?.collectEntries {
            [(it.id as String): it.name + ":" + it.blueprintName]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getBlueprintMap(String id) {
        def result = getBlueprint(id)?.collectEntries {
            if (it.key == "parameters") {
                it.value.collectEntries {
                    [(it.key as String): it.value as String]
                }
            } else if (it.key == "ambariBlueprint") {
                def result = it.value.host_groups?.collectEntries { [(it.name): it.components.collect { it.name }] }
                [(it.key as String): result as String]
            } else {
                [(it.key as String): it.value as String]
            }
        }
        result ?: new HashMap()
    }

    def Map<String, String> getTemplatesMap() {
        def result = getTemplates()?.collectEntries {
            [(it.id as String): it.name + ":" + it.description]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getTemplateMap(String id) {
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

    def Map<String, String> getStacksMap() {
        def result = getStacks()?.collectEntries {
            [(it.id as String): it.name + ":" + it.nodeCount]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getStackMap(String id) {
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

    def Map<String, String> getClustersMap() {
        def result = getClusters()?.collectEntries {
            [(it.id as String): it.cluster + ":" + it.status]
        }
        result ?: new HashMap()
    }

    def Map<String, String> getClusterMap(String id) {
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

    def List<Map> getTemplates() {
        log.debug("Getting templates...")
        getAllAsList(Resource.TEMPLATES)
    }

    def List<Map> getStacks() {
        log.debug("Getting templates...")
        getAllAsList(Resource.STACKS)
    }

    def List<Map> getClusters() {
        log.debug("Getting clusters...")
        getAllAsList(Resource.CLUSTERS)
    }

    def Object getStack(String id) {
        log.debug("Getting template...")
        return getOne(Resource.STACKS, id)
    }

    def Object getCluster(String id) {
        log.debug("Getting cluster...")
        return getOne(Resource.CLUSTERS, id)
    }

    def Object getCredential(String id) {
        log.debug("Getting credentials...")
        return getOne(Resource.CREDENTIALS, id)
    }

    def Object getTemplate(String id) {
        log.debug("Getting credentials...")
        return getOne(Resource.TEMPLATES, id)
    }

    def Object getBlueprint(String id) {
        log.debug("Getting credentials...")
        return getOne(Resource.BLUEPRINTS, id)
    }

    def private List getAllAsList(Resource resource) {
        Map getCtx = createGetRequestContext(resource.path(), [:]);
        Object response = doGet(getCtx);
        return response?.data
    }

    def private Object getOne(Resource resource, String id) {
        String path = resource.path() + "/$id"
        Map getCtx = createGetRequestContext(path, [:]);
        Object response = doGet(getCtx)
        return response?.data
    }

    def private Object doGet(Map getCtx) {
        Object response = null;
        try {
            response = restClient.get(getCtx)
        } catch (e) {
            log.error("ERROR: {}", e)
        }
        return response;
    }

    def private Object doPost(Map postCtx) {
        Object response = null;
        try {
            response = restClient.post(postCtx)
        } catch (e) {
            log.error("ERROR: {}", e)
        }
        return response;
    }

    def private Map createGetRequestContext(String resourcePath, Map ctx) {
        Map context = [:]
        String uri = "${restClient.uri}$resourcePath"
        context.put('path', uri)
        return context
    }

    def private Map createPostRequestContext(String resourcePath, Map ctx) {
        def Map<String, ?> putRequestMap = [:]
        def String uri = "${restClient.uri}$resourcePath"
        putRequestMap.put('path', uri)
        putRequestMap.put('body', ctx.get("json"));
        putRequestMap.put('requestContentType', ContentType.JSON)
        return putRequestMap
    }

    def private String createJson(String templateName, Map bindings) {
        def InputStream inPut = this.getClass().getClassLoader().getResourceAsStream("templates/${templateName}");
        String json = engine.createTemplate(new InputStreamReader(inPut)).make(bindings);
        return json;
    }

    def private Object processPost(Resource resource, Map binding) {
        def json = createJson(resource.template(), binding)
        def Map postCtx = createPostRequestContext(resource.path(), ['json': json])
        return doPost(postCtx)
    }

    private String getResourceContent(name) {
        getClass().getClassLoader().getResourceAsStream(name)?.text
    }
}

