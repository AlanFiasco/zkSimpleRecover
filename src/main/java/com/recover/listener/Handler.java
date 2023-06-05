package com.recover.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Handler {

    public Exporter exporter;
    public String dbName;
    public HashSet<String> allChildren;
    public CuratorCache cache;

    static final Map<String, HashSet<String>> cacheMap = new HashMap<>();

    public Handler(CuratorCache cache, String dbName, HashSet<String> allChildren) {
        this.cache = cache;
        this.dbName = dbName;
        this.allChildren = allChildren;
        exporter = new Exporter(cache, dbName, allChildren);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(exporter, 0, MetadataProperties.getInstance().getRECOVER_CHECKPOINT_TIMEOUT(), TimeUnit.MILLISECONDS);
    }

    public void onChanged(TreeCacheEvent event, LeaderLatch leaderLatch) {
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
        exporter.handleExport(paths, leaderLatch);

    }


}
