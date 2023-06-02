package com.recover.conn;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Connector {
    String url;
    String username;
    String password;

    public Connector() {
        try {
            final Properties properties = new Properties();
            final InputStream resource = Connector.class.getResourceAsStream("/jdbc.properties");
            properties.load(resource);
            url = properties.getProperty("url");
            username = properties.getProperty("username");
            password = properties.getProperty("password");
        } catch (IOException e) {
            throw new RuntimeException("connector.properties not found");
        }
    }

    public Connection getConnection() {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException("get connection failed", e);
        }
    }

    public ResultSet executeQuery(String sql) {
        Connection connection = getConnection();
        try {
            return connection.createStatement().executeQuery(sql);
        } catch (SQLException e) {
            throw new RuntimeException("execute query failed", e);
        }
    }

    public void executePrepareStatement(String sql, Object ... args) {
        Connection connection = getConnection();
        try {
            final PreparedStatement ps = connection.prepareStatement(sql);
            for (int i = 0; i <args.length; i++) {
                ps.setObject(i+1, args[i]);
            }
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("execute update failed", e);
        }
        closeConnection(connection);
    }

    public void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("close connection failed", e);
        }
    }
}
