import com.sequenceiq.cloudbreak.client.CloudbreakClient
import groovy.util.logging.Slf4j
import spock.lang.Specification


@Slf4j
class CloudbreakClientTest extends Specification {

    def CloudbreakClient cloudbreakClient = new CloudbreakClient('192.168.100.43', '8080', 'user@seq.com', 'test123');

    def "test health check"() {

        expect:
        String response = cloudbreakClient.health();
        log.debug("Health: {}", response)

    }

    def "test post credentials"() {

        expect:
        Object resp = cloudbreakClient.postBlueprint("Blueprint mamam")
        log.debug("RESP: {}", resp)
    }

    def "test get credentials"() {
        expect:
        Object resp = cloudbreakClient.getCredentials()
        log.debug("RESP: {}", resp)
    }

    def "test post blueprint"() {
        expect:
        Object resp = cloudbreakClient.postBlueprint(

        )
        log.debug("RESP: {}", resp)

    }

}