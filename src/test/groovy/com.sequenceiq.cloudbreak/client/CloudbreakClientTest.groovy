import com.sequenceiq.cloudbreak.client.CloudbreakClient
import groovy.util.logging.Slf4j
import org.junit.Ignore
import spock.lang.Specification

@Slf4j
@Ignore
class CloudbreakClientTest extends Specification {

        def CloudbreakClient cloudbreakClient = new CloudbreakClient('localhost', '9090', 'eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJlYTYzNzZkNy04NjRiLTQwZWYtOTg5OS00ZGM4MDFlYTZlNjEiLCJzdWIiOiI2OTg5NDZmMy00NTU4LTRmYTUtYWMxZS1iMjI5YTk0MGFjODMiLCJzY29wZSI6WyJjbG91ZGJyZWFrLnRlbXBsYXRlcyIsImNsb3VkYnJlYWsuY3JlZGVudGlhbHMiLCJjbG91ZGJyZWFrLnN0YWNrcyIsInBhc3N3b3JkLndyaXRlIiwib3BlbmlkIiwiY2xvdWRicmVhay5ibHVlcHJpbnRzIl0sImNsaWVudF9pZCI6ImNsb3VkYnJlYWtfc2hlbGwiLCJjaWQiOiJjbG91ZGJyZWFrX3NoZWxsIiwidXNlcl9pZCI6IjY5ODk0NmYzLTQ1NTgtNGZhNS1hYzFlLWIyMjlhOTQwYWM4MyIsInVzZXJfbmFtZSI6InBhdWwiLCJlbWFpbCI6InBhdWxAdGVzdC5vcmciLCJpYXQiOjE0MTEwNDcwMjAsImV4cCI6MTQxMTA5MDIyMCwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwL3VhYS9vYXV0aC90b2tlbiIsImF1ZCI6WyJjbG91ZGJyZWFrIiwib3BlbmlkIiwicGFzc3dvcmQiXX0.MsBd8ybhHs2P-aBLVgshsmOmnhW39-Ta9tTGsDY0_aU');

    def "test health check"() {

        expect:
        String response = cloudbreakClient.health();
        log.debug("Health: {}", response)

    }


    // TEMPLATES

    def "test post azure template"() {

        expect:
        Object resp = cloudbreakClient.postAzureTemplate("azuretempshell", "sdfsdfsd", "NORTH_EUROPE", "ambari-docker-v1", "MEDIUM", "2", "50")
        println resp
    }

    def "test post aws template"() {

        expect:
        Object resp = cloudbreakClient.postEc2Template("mytemplate", "my description", "EU_WEST_1", "ami-7778af00", "0.0.0.0/0", "T2Small", "2", "100", "Gp2");
        println resp
    }

    def "test get templates"() {
        expect:
        Object resp = cloudbreakClient.getTemplates()
        println resp
        Object mapResp = cloudbreakClient.getTemplatesMap()
        println mapResp
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


    // CREDENTIALS

    def "test post credential"() {
        expect:
        Object resp = cloudbreakClient.postEc2Credential("mycredential", "desc", "rolearn", "sshkey")
        println resp
    }

    def "test get credentials"() {
        expect:
        Object resp = cloudbreakClient.getCredentials()
        println resp
        Object mapResp = cloudbreakClient.getCredentialsMap()
        println mapResp
    }

    def "test get credential"() {
        expect:
        Object resp = cloudbreakClient.getCredential("50")
        println resp
        Object mapResp = cloudbreakClient.getCredentialMap("50")
        println mapResp
    }


    // BLUEPRINTS

    def "test post blueprint"() {
        expect:
        Object resp = cloudbreakClient.postBlueprint("bp1","desc",getClass().getClassLoader().getResourceAsStream("blueprints/multi-node-hdfs-yarn")?.text);
        println resp
    }

    def "test get blueprints"() {
        expect:
        Object resp = cloudbreakClient.getBlueprints()
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


    // STACKS

    def "test post cluster"() {
        expect:
        Object resp = cloudbreakClient.postCluster("test", 52, 51)
        log.debug("RESP: {}", resp)
    }

    def "test get stacks"() {
        expect:
        Object resp = cloudbreakClient.getStacks()
        println resp
        log.debug("RESP: {}", resp)
    }

}