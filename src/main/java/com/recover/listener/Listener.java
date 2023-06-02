package com.recover.listener;

import com.google.protobuf.InvalidProtocolBufferException;
import com.recover.conn.Connector;
import com.recover.protos.Node;
import com.recover.protos.NodeMessage;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static java.lang.System.in;

@Slf4j()
public class Listener {
    private final Clinet clinet;

    public Listener() {
        clinet = new Clinet();
    }

    public void listen() {
        String metadataPath = MetadataProperties.getInstance().LISTEN_PATH;
        //遍历metadataPath，对单个库进行监听
        final List<String> db_names = clinet.getChildren(metadataPath);
        for (String dbName : db_names) {
            clinet.addListener(metadataPath + "/" + dbName, new Handler());
        }
        Scanner sc = new Scanner(in);
        while (sc.hasNext()) {
            if ("recover".equalsIgnoreCase(sc.next())) {
                recover(sc);
            }
        }
    }

    private void recover(Scanner scanner) {
        System.out.println("请输入节点名,如: '/metadata/powersql_test'");
        final String db_name = scanner.next();
        final Connector connector = new Connector();
        String sql = String.format("select * from recover_test where db_name = '%s'", db_name);
        final ResultSet resultSet = connector.executeQuery(sql);
        try {
            while (resultSet.next()) {
                final NodeMessage nodeMessage = NodeMessage.parseFrom(resultSet.getBytes(2));
                final Map<String, Node> messageMap = nodeMessage.getMessageMap();
                for (Map.Entry<String, Node> entry : messageMap.entrySet()) {
                    final String key = entry.getKey();
                    final Node node = entry.getValue();
                    clinet.addOrSetNode(key, node.getContext());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("An exception occurred during traversing resultSet,"+e);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("An exception occurred during the deserialization of protobuf, "+e);
        }
    }
}
