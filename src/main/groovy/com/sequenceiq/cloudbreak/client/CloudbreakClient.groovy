package com.sequenceiq.cloudbreak.client

import groovy.json.JsonSlurper
import groovy.text.TemplateEngine
import groovy.util.logging.Slf4j
import groovyx.net.http.RESTClient

@Slf4j
class CloudbreakClient {

  def RESTClient restClient;
  def TemplateEngine engine;
  def slurper = new JsonSlurper()
  def String credentialsTemplate = ""


  CloudbreakClient(host = 'cloudbreak.sequenceiq.com', port = '80', user = 'user@seq.com', password = 'test123') {
    restClient = new RESTClient("http://${host}:${port}/" as String)
    restClient.headers['Authorization'] = 'Basic ' + "$user:$password".getBytes('iso-8859-1').encodeBase64()
  }


  def postCredential() {
    log.debug("Posting credentials ...")
  }

  def postTemplate() {
    log.debug("Posting template ...")
  }

  def postStack() {
    log.debug("Posting stack ...")
  }

  def postBluebrint() {
    log.debug("Posting blueprint ...")
  }

  def postCluster() {
    log.debug("Posting cluster ...")
  }

  def health() {
    log.debug("Checking health ...")
    Map getCtx = createGetRequestContext('health', null)
    Object healthObj = doGet(getCtx)
    return healthObj.data.status == 'ok'
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

  def private Map createGetRequestContext(String resourcePath, Object... args) {
    Map context = [:]
    String uri = "${restClient.uri}$resourcePath"
    context.put('path', uri)
    return context
  }

}
