package com.recover.listener;

import com.google.protobuf.InvalidProtocolBufferException;
import com.recover.conn.Connector;
import com.recover.protos.Node;
import com.recover.protos.NodeMessage;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Scanner;

public class DataRecovery {

    public ZKClient zkClient;
    public DataRecovery(ZKClient zkClient) {
        this.zkClient = zkClient;
    }
    public void recover() {
        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            if ("recover".equalsIgnoreCase(sc.next())) {
                recover(sc);
            }
        }
    }
    private void recover(Scanner sc) {
        if ("recover".equalsIgnoreCase(sc.next())) {
            System.out.println("请输入节点名,如: '/metadata/powersql_test'");
            final String db_name = sc.next();
            final Connector connector = new Connector();
            String sql = String.format("select * from recover_test where db_name = '%s'", db_name);
            final ResultSet resultSet = connector.executeQuery(sql);
            try {
                while (resultSet.next()) {
                    //反序列化
                    final NodeMessage nodeMessage = NodeMessage.parseFrom(resultSet.getBytes(2));
                    final Map<String, Node> messageMap = nodeMessage.getMessageMap();
                    for (Map.Entry<String, Node> entry : messageMap.entrySet()) {
                        final String key = entry.getKey();
                        final Node node = entry.getValue();
                        zkClient.addOrSetNode(key, node.getContext());
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("An exception occurred during traversing resultSet," + e);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException("An exception occurred during the deserialization of protobuf, " + e);
            }
        }
    }

}
