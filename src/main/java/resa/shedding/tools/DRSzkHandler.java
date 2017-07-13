package resa.shedding.tools;

import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kailin on 16/5/17.
 */
public class DRSzkHandler {

    private static CuratorFramework client = null;
    public static NodeCache nodeCache;
    public final static ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

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

//    public static void watchChild(String path) throws Exception {
//        nodeCache = new NodeCache(client, path);
//        nodeCache.getListenable().addListener(new NodeCacheListener() {
//
//            public void nodeChanged() throws Exception {
//                System.out.println("nimabi"+nodeCache.getCurrentData().getPath() + ":" + new String(nodeCache.getCurrentData().getData()));
//            }
//        }, EXECUTOR_SERVICE);
//        nodeCache.start();
//    }

    public static void start() throws Exception {
        client.start();
    }

    public static void close() throws Exception {
        client.delete().forPath("/drs");
        client.close();
    }
}

//import org.apache.storm.shade.org.apache.curator.RetryPolicy;
//import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
//import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;
//
///**
// * Created by kailin on 16/5/17.
// */
//public class DRSzkHandler {
//
//    private static CuratorFramework client = null;
//
//    private DRSzkHandler(String zkServer,int port,int sessionTimeoutMs,
//                         int connectionTimeoutMs,int baseSleepTimeMsint,int maxRetries) {
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMsint, maxRetries);
//        client = CuratorFrameworkFactory.builder().connectString(zkServer+":"+port).retryPolicy(retryPolicy)
//                .sessionTimeoutMs(sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs).build();
//    }
//
//    public static synchronized CuratorFramework newClient(String zkServer,int port,int sessionTimeoutMs,
//                                                          int connectionTimeoutMs,int baseSleepTimeMsint,int maxRetries) {
//        if (client == null) {
//            new DRSzkHandler(zkServer, port, sessionTimeoutMs,
//                    connectionTimeoutMs, baseSleepTimeMsint, maxRetries);
//        }
//        return client;
//    }
//
//    public static void start() {
//        if (client != null) {
//            client.start();
//        }
//    }
//
//    public static void close() throws Exception {
//       // client.delete().forPath("/drs");
//        if (client != null) {
//            client.close();
//        }
//    }
//}
