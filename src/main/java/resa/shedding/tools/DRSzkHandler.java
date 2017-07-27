package resa.shedding.tools;

import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kailin on 16/5/17.
 */
public class DRSzkHandler {
    public static org.slf4j.Logger LOG = LoggerFactory.getLogger(DRSzkHandler.class);
    private static CuratorFramework client = null;
    public final static ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();
    public static final char PATH_SEPARATOR_CHAR = '/';

    private DRSzkHandler(String zkServer, int port, int sessionTimeoutMs,
                         int connectionTimeoutMs, int baseSleepTimeMsint, int maxRetries) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMsint, maxRetries);
        client = CuratorFrameworkFactory.builder().connectString(zkServer+":"+port).retryPolicy(retryPolicy)
                .sessionTimeoutMs(sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs).build();

    }

    public static synchronized CuratorFramework newClient(String zkServer,int port,int sessionTimeoutMs,
                                                          int connectionTimeoutMs,int baseSleepTimeMsint,int maxRetries) {
        if (client == null) {
            new DRSzkHandler(zkServer, port, sessionTimeoutMs,
                    connectionTimeoutMs, baseSleepTimeMsint, maxRetries);
        }
        return client;
    }

    public static void start() throws Exception {
        client.start();
    }

    public static void close(String path) throws Exception {
        deletePath(path,true);
        client.close();
        EXECUTOR_SERVICE.shutdownNow();
    }

    private static void deletePath(String path, boolean clearChildren) {
        try {
            if (client.isStarted() && client.checkExists().forPath(path) != null) {
                if (clearChildren) {

                    List<String> children = client.getChildren().forPath(path);
                    if (!children.isEmpty()) {

                        for (String childPath : children) {
                            deletePath(path + PATH_SEPARATOR_CHAR + childPath, true);
                        }
                    }
                }
                client.delete().forPath(path);
            } else {
                LOG.info("no path of "+path);
            }
        } catch (Exception e) {
            String message = "delete path failed, path = " + path;
            LOG.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}