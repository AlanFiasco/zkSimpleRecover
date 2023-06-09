package com.recover.listener;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ZKClient {
    private final MetadataProperties metadataProperties = MetadataProperties.getInstance();
    @Getter
    private final CuratorFramework client;
    int RETRY_INTERVAL_MS = metadataProperties.RETRY_INTERVAL_MS;
    int MAX_RETRIES = metadataProperties.MAX_RETRIES;
    int SESSION_TIMEOUT_MS = metadataProperties.SESSION_TIMEOUT_MS;
    int CONNECTION_TIMEOUT_MS = metadataProperties.CONNECTION_TIMEOUT_MS;
    String connectString = metadataProperties.CONNECT_STRING;
    String namespace = metadataProperties.NAMESPACE;

    public ZKClient() {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString(connectString).retryPolicy(new ExponentialBackoffRetry(RETRY_INTERVAL_MS, MAX_RETRIES, RETRY_INTERVAL_MS * MAX_RETRIES)).namespace(namespace).sessionTimeoutMs(SESSION_TIMEOUT_MS).connectionTimeoutMs(CONNECTION_TIMEOUT_MS);
        client = builder.build();
        initCuratorClient();
    }

    public List<String> getChildren(String key) {
        try {
            final List<String> result = client.getChildren().forPath(key);
            result.sort(Comparator.reverseOrder());
            return result;
        } catch (Exception e) {
            log.error("can't get children " + key, e);
            return Collections.emptyList();
        }
    }

    private HashSet<String> getAllChildren(String key) {
        HashSet<String> result = new HashSet<>();
        try {
            recursiveGetChildren(key, result);
        } catch (Exception e) {
            log.error("can't get all children " + key, e);
        }
        return result;
    }


    private void recursiveGetChildren(String key, Set<String> result) throws Exception {
        List<String> children = client.getChildren().forPath(key);
        if (children == null || children.isEmpty()) {
            result.add(key);
            return;
        }
        for (String child : children) {
            String childPath = key + "/" + child;
            recursiveGetChildren(childPath, result);
        }
        result.add(key);
    }

    private void initCuratorClient() {
        client.start();
        // 等待连接建立
        try {
            if (!client.blockUntilConnected(RETRY_INTERVAL_MS * MAX_RETRIES, TimeUnit.MILLISECONDS)) {
                client.close();
                throw new KeeperException.OperationTimeoutException();
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        } catch (final KeeperException.OperationTimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void addOrSetNode(final String key, final String value) {
        try {
            if (null != client.checkExists().forPath(key)) {
                setValue(key, value);
            } else {
                client.create().creatingParentsIfNeeded().forPath(key, value.getBytes());
            }
        } catch (Exception e) {
            log.error("can't add node " + key + " to " + value, e);
        }
    }

    public void setValue(final String key, final String value) {
        try {
            client.setData().forPath(key, value.getBytes());
        } catch (Exception e) {
            log.error("can't set value " + key + " to " + value, e);
        }
    }

    public void addListener(final String rawKey, LeaderLatch leaderLatch) {
        String key = rawKey.toLowerCase();
        CuratorCache cache = CuratorCache.build(client, key);
        final HashSet<String> allChildren = getAllChildren(rawKey);
        final Handler handler = new Handler(cache, key, allChildren);
        CuratorCacheListener curatorCacheListener = CuratorCacheListener.builder().forTreeCache(client, (framework, treeCacheEvent) -> {
            TreeCacheEvent.Type changedType = treeCacheEvent.getType();
            switch (changedType) {
                case NODE_ADDED:
                case NODE_UPDATED:
                case NODE_REMOVED: {
                    handler.onChanged(treeCacheEvent, leaderLatch);
                    break;
                }
                default:
                    break;
            }
        }).build();
        cache.listenable().addListener(curatorCacheListener);
        start(cache);

    }


    private void start(final CuratorCache cache) {
        try {
            cache.start();
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
