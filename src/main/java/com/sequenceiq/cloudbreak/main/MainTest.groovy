package com.sequenceiq.cloudbreak.main

import com.sequenceiq.cloudbreak.client.CloudbreakClient

public class MainTest {

    public static void main(String[] args) throws IOException {
        def token = "<INSERT_TOKEN>"
        CloudbreakClient client = new CloudbreakClient("http://localhost:9090", token);

        def name = "clustername"

        // create stack
        def ins = ["master"   : ["templateId": 50, "nodeCount": 1, "type": "CORE"],
                   "slave_1"  : ["templateId": 50, "nodeCount": 1, "type": "CORE"],
                   "cbgateway": ["templateId": 50, "nodeCount": 1, "type": "GATEWAY"]]
        def stackId = client.postStack(name, "50", "US_CENTRAL1_A", false, ins, "DO_NOTHING", 2, "BEST_EFFORT", null, "52")

        // create cluster
        List<Map<String, Object>> hostGroups = [["name": "master", "instanceGroupName": "master"], ["name": "slave_1", "instanceGroupName": "slave_1"]]
        client.postCluster(name, "apple", "apple", 50, "desc", stackId as Integer, hostGroups, "HDP", "2.2", "redhat6",
                "HDP-2.2", "http://public-repo-1.hortonworks.com/HDP/centos6/2.x/GA/2.2.0.0",
                "HDP-UTILS-1.1.0.20", "http://public-repo-1.hortonworks.com/HDP-UTILS-1.1.0.20/repos/centos6", true)

    }
}
