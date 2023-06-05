package com.recover.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Connector {
    private final ConnProperties properties = ConnProperties.getInstance();
    String url = properties.getURL();
    String username = properties.getUSERNAME();
    String password = properties.getPASSWORD();

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
