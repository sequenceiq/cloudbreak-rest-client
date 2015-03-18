package com.sequenceiq.cloudbreak.main;

import static com.sequenceiq.cloudbreak.main.VersionedApplication.versionedApplication;

import java.io.IOException;

public class CloudbreakClientMain {

    public static void main(String[] args) throws IOException {
        versionedApplication().showVersionInfo(args);
    }
}
