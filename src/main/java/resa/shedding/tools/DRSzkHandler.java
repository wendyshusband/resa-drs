package resa.shedding.tools;

import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by kailin on 16/5/17.
 */
public class DRSzkHandler {

    private static CuratorFramework client = null;

    private DRSzkHandler(String zkServer,int port,int sessionTimeoutMs,
                         int connectionTimeoutMs,int baseSleepTimeMsint,int maxRetries) {
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

    public static void start() {
        client.start();
    }

    public static void close() throws Exception {
        client.delete().forPath("/drs");
        client.close();
    }
}
