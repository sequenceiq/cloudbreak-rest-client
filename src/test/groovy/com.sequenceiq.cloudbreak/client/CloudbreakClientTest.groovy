import com.sequenceiq.cloudbreak.client.CloudbreakClient
import groovy.util.logging.Slf4j
import spock.lang.Specification

@Slf4j
class CloudbreakClientTest extends Specification {

        def CloudbreakClient cloudbreakClient = new CloudbreakClient('localhost', '9090', 'eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJlYTYzNzZkNy04NjRiLTQwZWYtOTg5OS00ZGM4MDFlYTZlNjEiLCJzdWIiOiI2OTg5NDZmMy00NTU4LTRmYTUtYWMxZS1iMjI5YTk0MGFjODMiLCJzY29wZSI6WyJjbG91ZGJyZWFrLnRlbXBsYXRlcyIsImNsb3VkYnJlYWsuY3JlZGVudGlhbHMiLCJjbG91ZGJyZWFrLnN0YWNrcyIsInBhc3N3b3JkLndyaXRlIiwib3BlbmlkIiwiY2xvdWRicmVhay5ibHVlcHJpbnRzIl0sImNsaWVudF9pZCI6ImNsb3VkYnJlYWtfc2hlbGwiLCJjaWQiOiJjbG91ZGJyZWFrX3NoZWxsIiwidXNlcl9pZCI6IjY5ODk0NmYzLTQ1NTgtNGZhNS1hYzFlLWIyMjlhOTQwYWM4MyIsInVzZXJfbmFtZSI6InBhdWwiLCJlbWFpbCI6InBhdWxAdGVzdC5vcmciLCJpYXQiOjE0MTEwNDcwMjAsImV4cCI6MTQxMTA5MDIyMCwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwL3VhYS9vYXV0aC90b2tlbiIsImF1ZCI6WyJjbG91ZGJyZWFrIiwib3BlbmlkIiwicGFzc3dvcmQiXX0.MsBd8ybhHs2P-aBLVgshsmOmnhW39-Ta9tTGsDY0_aU');

    def "test health check"() {

        expect:
        String response = cloudbreakClient.health();
        log.debug("Health: {}", response)

    }

    def "test post azuretemplate"() {

        expect:
        Object resp = cloudbreakClient.postAzureTemplate("azuretempshell", "sdfsdfsd", "NORTH_EUROPE", "ambari-docker-v1", "MEDIUM", "sdfdsfsdfsdfsf", "")
        log.debug("RESP: {}", resp)
    }

    def "test post template"() {

        expect:
        Object resp = cloudbreakClient.postEc2Template("mytemplate", "my description", "EU_WEST_1", "ami-7778af00", "0.0.0.0/0", "T2Small", "2", "100", "Gp2");
        log.debug("RESP: {}", resp)
    }

    def "test get credentials"() {
        expect:
        Object resp = cloudbreakClient.getCredentials()
        log.debug("RESP: {}", resp)
    }

    def "test get credential"() {
        expect:
        Object resp = cloudbreakClient.getCredential("50")
        Object resp1 = cloudbreakClient.getCredentialMap("50")
        log.debug("RESP: {}", resp)
        log.debug("RESP1: {}", resp1)
    }

    def "test post blueprint"() {
        expect:
        Object resp = cloudbreakClient.postBlueprint(

        )
        log.debug("RESP: {}", resp)
    }

    def "test post cluster"() {
        expect:
        Object resp = cloudbreakClient.postCluster("test", 52, 51)
        log.debug("RESP: {}", resp)
    }


    def "test get blueprints as map"() {
        expect:
        Object resp = cloudbreakClient.getBlueprintsMap()
        log.debug("RESP: {}", resp)
    }

    def "test get credentials as map"() {
        expect:
        Object resp = cloudbreakClient.getCredentialsMap()
        log.debug("RESP: {}", resp)
    }

    def "test get templates as map"() {
        expect:
        Object resp = cloudbreakClient.getTemplatesMap()
        log.debug("RESP: {}", resp)
    }

    def "test get stacks as map"() {
        expect:
        Object resp = cloudbreakClient.getStacksMap()
        log.debug("RESP: {}", resp)
    }

    def "test get clusters as map"() {
        expect:
        Object resp = cloudbreakClient.getClustersMap()
        log.debug("RESP: {}", resp)
    }

    def "test get login"() {
        expect:
        Object resp = cloudbreakClient.login()
        log.debug("RESP: {}", resp)
    }

}