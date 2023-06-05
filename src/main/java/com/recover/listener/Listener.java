package com.recover.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.leader.LeaderLatch;


import java.util.List;


@Slf4j()
public class Listener {
    private final ZKClient zkClient;
    private final LeaderLatch leaderLatch;

    public Listener(ZKClient zkClient, LeaderLatch leaderLatch) {
        this.zkClient = zkClient;
        this.leaderLatch = leaderLatch;
    }

    public void listen() {
        String metadataPath = MetadataProperties.getInstance().LISTEN_PATH;
        //遍历metadataPath，对单个库进行监听
        final List<String> db_names = zkClient.getChildren(metadataPath);
        for (String dbName : db_names) {
            final String rawKey = metadataPath + "/" + dbName;
            zkClient.addListener(rawKey, leaderLatch);
        }
    }

}
