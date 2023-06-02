package com.recover.listener;

import com.recover.conn.Connector;
import com.recover.protos.Node;
import com.recover.protos.NodeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Slf4j
public class Handler {

    final static Map<String, Integer> TRIGGER_COUNT_MAP = new HashMap<>();

    final static Integer TRIGGER_THRESHOLD = MetadataProperties.getInstance().getRECOVER_TRIGGER_THRESHOLD();
    final static Integer TIME_WINDOW = MetadataProperties.getInstance().getRECOVER_TIME_WINDOW();

    final static Map<String, Long> TIME_WINDOW_MAP = new HashMap<>();
    final static Map<String, HashSet<String>> cacheMap = new HashMap<>();

    public void onChanged(CuratorCache cache, String dbName, TreeCacheEvent event, LeaderLatch leaderLatch) {
        HashSet<String> paths = cacheMap.get(dbName);
        if (null == paths) {
            paths = new HashSet<>();
        }
        if (event.getType() == TreeCacheEvent.Type.NODE_ADDED) {
            paths.add(event.getData().getPath());
            cacheMap.put(dbName, paths);
        } else if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED) {
            paths.remove(event.getData().getPath());
            cacheMap.put(dbName, paths);
        }
        if (isThresholdMet(dbName)) {
            if (leaderLatch.hasLeadership()) {
                // 只有Leader节点才能执行的操作
                exportData(cache, dbName, paths);
            } else {
                log.info("Need leader to execute export data");
            }
        }
    }

    private boolean isThresholdMet(String path) {
        final long duration = System.currentTimeMillis() - TIME_WINDOW_MAP.getOrDefault(path, System.currentTimeMillis());
        Integer triggerCount = TRIGGER_COUNT_MAP.get(path);
        if (triggerCount == null) {
            TRIGGER_COUNT_MAP.put(path, 0);
            TIME_WINDOW_MAP.put(path, System.currentTimeMillis());
            return false;
        } else if (triggerCount >= TRIGGER_THRESHOLD && duration < TIME_WINDOW) {
            TRIGGER_COUNT_MAP.put(path, 0);
            return true;
        } else if (duration >= TIME_WINDOW) {
            TIME_WINDOW_MAP.put(path, System.currentTimeMillis());
            TRIGGER_COUNT_MAP.put(path, 0);
            return false;
        }
        triggerCount++;
        TRIGGER_COUNT_MAP.put(path, triggerCount);
        return false;
    }

    private void exportData(CuratorCache cache, String dbName, HashSet<String> paths) {
        final NodeMessage.Builder messageBuilder = NodeMessage.newBuilder();
        for (String path : paths) {
            if (cache.get(path).isPresent()) {
                if (path.startsWith(MetadataProperties.getInstance().getIGNORE_DIRECTORY()) || path.equalsIgnoreCase(MetadataProperties.getInstance().getIGNORE_NODE())) {
                    continue;
                }
                String context = new String(cache.get(path).get().getData());
                final Node node = Node.newBuilder().setDatabaseName(dbName).setPath(path).setContext(context).build();
                messageBuilder.putMessage(path, node);
            }
        }
        final Connector connector = new Connector();
        final NodeMessage nodeMassage = messageBuilder.build();
        final byte[] byteArray = nodeMassage.toByteArray();
        String sql = "replace into recover_test values(?,?)";
        connector.executePrepareStatement(sql, dbName, byteArray);
    }

}
