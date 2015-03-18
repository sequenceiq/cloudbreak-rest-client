package com.sequenceiq.cloudbreak.main;

import java.io.IOException;

public class CloudbreakClientMain extends VersionedApplication {

    public static void main(String[] args) throws IOException {
        start(CloudbreakClientMain.class, args);
    }
}
