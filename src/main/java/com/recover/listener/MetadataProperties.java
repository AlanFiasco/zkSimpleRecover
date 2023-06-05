package com.recover.listener;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Getter
public class MetadataProperties {

    int RETRY_INTERVAL_MS;
    int MAX_RETRIES;
    int SESSION_TIMEOUT_MS;
    int CONNECTION_TIMEOUT_MS;
    String CONNECT_STRING;
    String NAMESPACE;
    String LISTEN_PATH;
    int RECOVER_TIME_WINDOW;
    int RECOVER_TRIGGER_THRESHOLD;
    int RECOVER_CHECKPOINT_TIMEOUT;
    String  IGNORE_NODE;
    String IGNORE_PATH;

    private MetadataProperties() {
        Properties properties = new Properties();
        final InputStream resource = ZKClient.class.getResourceAsStream("/metadata.properties");
        try {
            properties.load(resource);
        } catch (IOException e) {
            throw new RuntimeException("metadata.properties not found");
        }
        try {
            RETRY_INTERVAL_MS = Integer.parseInt(properties.getProperty("retryIntervalMs"));
            MAX_RETRIES = Integer.parseInt(properties.getProperty("maxRetries"));
            SESSION_TIMEOUT_MS = Integer.parseInt(properties.getProperty("sessionTimeoutMs"));
            CONNECTION_TIMEOUT_MS = Integer.parseInt(properties.getProperty("connTimeoutMs"));
            CONNECT_STRING = properties.getProperty("connectString");
            NAMESPACE = properties.getProperty("namespace");
            LISTEN_PATH = properties.getProperty("listenPath");
            RECOVER_TIME_WINDOW = Integer.parseInt(properties.getProperty("recoverTimeWindow"));
            RECOVER_TRIGGER_THRESHOLD = Integer.parseInt(properties.getProperty("recoverTriggerThreshold"));
            RECOVER_CHECKPOINT_TIMEOUT = Integer.parseInt(properties.getProperty("recoverCheckpointTimeout"));
//            IGNORE_NODE = properties.getProperty("ignoreNode");
            IGNORE_PATH = properties.getProperty("ignorePath");
        } catch (Exception e) {
            throw new RuntimeException("load metadata.properties failed");
        }
    }

    private static class MetadataPropertiesInstanceHolder {
        private static final MetadataProperties INSTANCE = new MetadataProperties();
    }

    public static MetadataProperties getInstance() {
        return MetadataPropertiesInstanceHolder.INSTANCE;
    }
}
