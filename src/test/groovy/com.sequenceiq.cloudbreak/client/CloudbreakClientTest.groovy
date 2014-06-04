import com.sequenceiq.cloudbreak.client.CloudbreakClient
import groovy.util.logging.Slf4j
import spock.lang.Specification


@Slf4j
class CloudbreakClientTest extends Specification {

    def CloudbreakClient cloudbreakClient = new CloudbreakClient();

    def "test health check"() {

        expect:
        String response = cloudbreakClient.health();
        log.debug("Health: {}", response)

    }

    def "test post credentials"() {

        expect:
        cloudbreakClient.postBlueprint("blueprint")
    }

}