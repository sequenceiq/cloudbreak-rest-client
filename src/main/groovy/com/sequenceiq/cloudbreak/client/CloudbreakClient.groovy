package com.sequenceiq.cloudbreak.client

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import groovyx.net.http.RESTClient

@Slf4j
class CloudbreakClient {

    def RESTClient ambari
    def slurper = new JsonSlurper()

    CloudbreakClient(host = 'localhost', port = '8080', user = 'user@seq.com', password = 'test123') {
        ambari = new RESTClient("http://${host}:${port}/" as String)
        ambari.headers['Authorization'] = 'Basic ' + "$user:$password".getBytes('iso-8859-1').encodeBase64()
    }
}
