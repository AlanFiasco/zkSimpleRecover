package com.recover.listener;

import com.recover.conn.ConnProperties;
import com.recover.conn.Connector;
import com.recover.protos.Node;
import com.recover.protos.NodeMessage;
import com.recover.util.SnowFlake;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Slf4j
public class DataExporter implements Runnable {
    private final String dbNamePath;
    private final CuratorCache cache;
    private final HashSet<String> allDbPaths;

    private boolean hasExporter = false;
    private static final SnowFlake snowFlake = new SnowFlake(ConnProperties.getInstance().getSNOWFLAKE_DATA_CENTER_ID(), ConnProperties.getInstance().getSNOWFLAKE_MACHINE_ID());

    static final Map<String, Integer> TRIGGER_COUNT_MAP = new HashMap<>();

    static final Integer TRIGGER_THRESHOLD = MetadataProperties.getInstance().getRECOVER_TRIGGER_THRESHOLD();
    static final Integer TIME_WINDOW = MetadataProperties.getInstance().getRECOVER_TIME_WINDOW();

    static final Map<String, Long> TIME_WINDOW_MAP = new HashMap<>();

    public DataExporter(CuratorCache cache, String dbNamePath, HashSet<String> allDbPaths) {
        this.cache = cache;
        this.dbNamePath = dbNamePath;
        this.allDbPaths = allDbPaths;
    }

    @Override
    public void run() {
        if (!hasExporter) {
            exportData(allDbPaths);
        }
    }

    public void handleExport(HashSet<String> paths, LeaderLatch leaderLatch) {
        if (isThresholdMet()) {
            if (leaderLatch.hasLeadership()) {
                // 只有Leader节点才能执行的操作
                exportData(paths);
            } else {
                log.info("Need leader to execute export data");
            }
        }
    }

    private boolean isThresholdMet() {
        final long duration = System.currentTimeMillis() - TIME_WINDOW_MAP.getOrDefault(dbNamePath, System.currentTimeMillis());
        Integer triggerCount = TRIGGER_COUNT_MAP.get(dbNamePath);
        if (triggerCount == null) {
            TRIGGER_COUNT_MAP.put(dbNamePath, 0);
            TIME_WINDOW_MAP.put(dbNamePath, System.currentTimeMillis());
            return false;
        } else if (triggerCount >= TRIGGER_THRESHOLD && duration < TIME_WINDOW) {
            TRIGGER_COUNT_MAP.put(dbNamePath, 0);
            hasExporter = true;
            return true;
        } else if (duration >= TIME_WINDOW) {
            TIME_WINDOW_MAP.put(dbNamePath, System.currentTimeMillis());
            TRIGGER_COUNT_MAP.put(dbNamePath, 0);
            hasExporter = false;
            return false;
        }
        triggerCount++;
        TRIGGER_COUNT_MAP.put(dbNamePath, triggerCount);
        return false;
    }

    private void exportData(HashSet<String> paths) {
        final NodeMessage.Builder messageBuilder = NodeMessage.newBuilder();
        final String ignorePath = MetadataProperties.getInstance().getIGNORE_PATH();
        for (String path : paths) {
            if (cache.get(path).isPresent()) {
                if (path.startsWith(ignorePath)) {
                    continue;
                }
                String context = new String(cache.get(path).get().getData());
                //构造单个节点
                final Node node = Node.newBuilder().setDatabaseName(dbNamePath).setPath(path).setContext(context).build();
                messageBuilder.putMessage(path, node);
            }
        }
        final Connector connector = new Connector();
        //构造库下所有节点
        final NodeMessage nodeMassage = messageBuilder.build();
        //序列化
        final byte[] byteArray = nodeMassage.toByteArray();
        final long snowFlakeId = snowFlake.getNextId();
        String sql = "insert into zk_backup values(?,?,?,?,?)";
        connector.executePrepareStatement(sql, snowFlakeId, dbNamePath, byteArray, ignorePath, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()));
    }


}
