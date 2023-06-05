package com.recover.conn;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Getter
public class ConnProperties {
    private final Long SNOWFLAKE_DATA_CENTER_ID;
    private final Long SNOWFLAKE_MACHINE_ID;

    private final String URL;
    private final String USERNAME;
    private final String PASSWORD;

    private ConnProperties() {
        Properties props = new Properties();
        final InputStream resource = ConnProperties.class.getResourceAsStream("/jdbc.properties");
        try {
            props.load(resource);
            SNOWFLAKE_DATA_CENTER_ID = Long.parseLong(props.getProperty("snowflakeDataCenterId"));
            SNOWFLAKE_MACHINE_ID = Long.parseLong(props.getProperty("snowflakeMachineId"));
            URL = props.getProperty("url");
            USERNAME = props.getProperty("username");
            PASSWORD = props.getProperty("password");
        } catch (IOException e) {
            throw new RuntimeException("load jdbc properties failed, " + e);
        }
    }

    private static class ConnPropertiesInstanceHolder {
        private static final ConnProperties INSTANCE = new ConnProperties();
    }

    public static ConnProperties getInstance() {
        return ConnPropertiesInstanceHolder.INSTANCE;
    }
}
