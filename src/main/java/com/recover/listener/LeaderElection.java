package com.recover.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

@Slf4j
public class LeaderElection {
    public LeaderLatch leaderLatch;
    public LeaderElection(ZKClient zkClient) {
        leaderLatch = new LeaderLatch(zkClient.getClient(), "/zkRecoverElection");
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                // 只有Leader会触发此方法
                log.info("isLeader");
            }
            @Override
            public void notLeader() {
                // 非Leader会触发此方法
                log.info("Not Leader");
            }
        });
        try {
            leaderLatch.start();
        } catch (Exception e) {
            throw new RuntimeException("leaderLatch start failed " + e);
        }
    }
}
