package com.recover.bootstrap;

import com.recover.listener.DataRecovery;
import com.recover.listener.LeaderElection;
import com.recover.listener.Listener;
import com.recover.listener.ZKClient;


public class Main {
    public static void main(String[] args) {
        final ZKClient zkClient = new ZKClient();
        final LeaderElection leader = new LeaderElection(zkClient);
        final Listener listener = new Listener(zkClient,leader.leaderLatch);
        listener.listen();
        final DataRecovery dataRecovery = new DataRecovery(zkClient);
        dataRecovery.recover();
    }
}
