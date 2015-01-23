import com.sequenceiq.cloudbreak.client.CloudbreakClient
import groovy.util.logging.Slf4j
import spock.lang.Ignore
import spock.lang.Specification

@Slf4j
@Ignore
class CloudbreakClientTest extends Specification {

    def CloudbreakClient cloudbreakClient = new CloudbreakClient('localhost:9090', '');

    def "test health check"() {

        expect:
        String response = cloudbreakClient.health();
        log.debug("Health: {}", response)

    }

    def "test post stack"() {
        Map<String, Map.Entry<Long, Integer>> map = new HashMap<>()
        Map.Entry<Long,Integer> entry1 =
                new AbstractMap.SimpleEntry<Long, Integer>(1l, 1);
        Map.Entry<Long,Integer> entry2 =
                new AbstractMap.SimpleEntry<Long, Integer>(1l, 1);
        map.put("master", entry1);
        map.put("slave_1", entry2);
        expect:
        Object resp = cloudbreakClient.postStack("mystack", "admin", "admin", "59", "EU_WEST", true, map);
        println resp
    }


    // USER_TEMPLATES

    def "test post azure template"() {

        expect:
        Object resp = cloudbreakClient.postAzureTemplate("azuretempshell2", "sdfsdfsd", "ambari-docker-v1", "MEDIUM", "2", "50", false)
        println resp
    }

    def "test post aws template"() {

        expect:
        Object resp = cloudbreakClient.postEc2Template("mytemplate", "my description", "0.0.0.0/0", "T2Small", "2", "100", "Gp2", true);
        println resp
    }

    def "test get templates"() {
        expect:
        Object resp = cloudbreakClient.getPrivateTemplates()
        println resp
        Object mapResp = cloudbreakClient.getPrivateTemplatesMap()
        println mapResp
        Object accResp = cloudbreakClient.getAccountTemplates()
        println accResp
        Object accMapResp = cloudbreakClient.getAccountTemplatesMap()
        println accMapResp
    }

    def "test get template"() {
        expect:
        Object resp = cloudbreakClient.getTemplate("50")
        println resp
        Object mapResp = cloudbreakClient.getTemplateMap("50")
        println mapResp
    }

    def "test delete template"() {
        expect:
        Object resp = cloudbreakClient.deleteTemplate("50")
        println resp
    }


    // USER_CREDENTIALS

    def "test post credential"() {
        expect:
        Object resp = cloudbreakClient.postEc2Credential("mycredential", "desc", "rolearn", "sshkey")
        println resp
    }

    def "test get credentials"() {
        expect:
        Object resp = cloudbreakClient.getPrivateCredentials()
        println resp
        Object mapResp = cloudbreakClient.getPrivateCredentialsMap()
        println mapResp
    }

    def "test get credential"() {
        expect:
        Object resp = cloudbreakClient.getCredential("50")
        println resp
        Object mapResp = cloudbreakClient.getCredentialMap("50")
        println mapResp
    }


    // USER_BLUEPRINTS

    def "test post blueprint"() {
        expect:
        Object resp = cloudbreakClient.postBlueprint("bp1","desc",getClass().getClassLoader().getResourceAsStream("blueprints/multi-node-hdfs-yarn")?.text);
        println resp
    }

    def "test get blueprints"() {
        expect:
        Object resp = cloudbreakClient.getPrivateBlueprints()
        println resp
        Object mapResp = cloudbreakClient.getBlueprintsMap()
        println mapResp
    }

    def "test get blueprint"() {
        expect:
        Object resp = cloudbreakClient.getBlueprint("50")
        println resp
        Object mapResp = cloudbreakClient.getBlueprintMap("50")
        println mapResp
    }


    // USER_STACKS

    def "test post cluster"() {
        expect:
        Object resp = cloudbreakClient.postCluster("test", 52, 51)
        log.debug("RESP: {}", resp)
    }

    def "test get stacks"() {
        expect:
        Object resp = cloudbreakClient.getPrivateStacks()
        println resp
        log.debug("RESP: {}", resp)
    }

}