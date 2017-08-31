package resa.shedding.tools;

import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kailin on 16/5/17.
 */
public class DRSzkHandler {

    public enum lastDecision {
        DECISIONMAKE(1), TRIM(0), FIRSTSTATUS(-1);
        private final int value;

        lastDecision(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static String getStatus(int value){
            switch (value){
                case 0: return "TRIM";
                case 1: return "FIRSTSTATUS";
                case -1: return "FIRST";
                default: return "unknown";
            }
        }
    }

    public volatile static lastDecision decision = lastDecision.FIRSTSTATUS;
    public static org.slf4j.Logger LOG = LoggerFactory.getLogger(DRSzkHandler.class);
    private static CuratorFramework client = null;
    public final static ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();
    public static final char PATH_SEPARATOR_CHAR = '/';
    private static Map<String, NodeCache> nodeCaches = new HashMap<>();
    private static Map<String, Integer> nodeCacheIsStarted = new HashMap<>();
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

    public static boolean clientIsStart() {
        return client.isStarted();
    }

    public static void start() throws Exception {
        client.start();
    }

    public static void close(String path) throws Exception {
        nodeCaches.values().stream().forEach(e -> {
            try {
                e.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        });
        EXECUTOR_SERVICE.shutdownNow();
        //deletePath(path,true);
        client.close();
    }

    private static void deletePath(String path, boolean clearChildren) {
        try {
            if (clientIsStart() && client.checkExists().forPath(path) != null) {
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

    public static synchronized void sentActiveSheddingRate(Map<String,Double> activeSheddingRateMap, String topologyName, lastDecision mkDecision) throws Exception {
        if (mkDecision == lastDecision.TRIM && decision == lastDecision.DECISIONMAKE) {
            setDecision(lastDecision.TRIM);
            LOG.info("DRS had just mk decision, skip this trim!");
            return;
        } else if (mkDecision == lastDecision.TRIM && decision == lastDecision.FIRSTSTATUS) {
            LOG.info("DRS not to have any decision, skip this trim!");
            return;
        } else {
            setDecision(mkDecision);
        }
        if (!clientIsStart()) {
            DRSzkHandler.start();
        }
        if(client.checkExists().forPath("/drs/"+topologyName) == null){
            client.create().creatingParentsIfNeeded().forPath("/drs/"+topologyName,activeSheddingRateMap.toString().getBytes());
        }else{
            client.setData().forPath("/drs/"+topologyName,activeSheddingRateMap.toString().getBytes());
        }
        System.out.println("teng: "+new String(client.getData().forPath("/drs/"+topologyName)));
    }

    public static double parseActiveShedRateMap(byte[] activeShedRateMapBytes, String compID) {
        String tempMap = new String(activeShedRateMapBytes);
        System.out.println(tempMap+"paris3"+compID);
        Pattern pattern1 = Pattern.compile("[\\s{]"+compID + "=(\\d+)\\.(\\d+)");
        Matcher matcher1 = pattern1.matcher(tempMap);
        if (matcher1.find()) {
            System.out.println(matcher1.toString()+"paris4"+compID);
            Pattern pattern2 = Pattern.compile("(\\d+)\\.(\\d+)");
            Matcher matcher2 = pattern2.matcher(matcher1.group());
            if (matcher2.find()) {
                System.out.println(matcher1.toString()+"paris5"+compID);
                return Double.valueOf(matcher2.group());
            }
        }
        return Double.MAX_VALUE;
    }

    public static NodeCache createNodeCache (String path) throws Exception {
        if (nodeCaches.containsKey(path)) {
            System.out.println(nodeCaches.get(path).getListenable().size()+"youle"+nodeCaches.get(path).getListenable().toString());
            return nodeCaches.get(path);
        } else {
            NodeCache nodeCache = new NodeCache(client, path);
            nodeCaches.put(path, nodeCache);
            nodeCaches.put(path, nodeCache);
            nodeCache.start();
            return nodeCache;
        }
    }

    public static void rebuildNodeCache (String path) throws Exception {
        if (nodeCaches.containsKey(path)) {
            nodeCaches.get(path).rebuild();
        }
    }

    public static lastDecision getDecision () {
        return decision;
    }
    public static void setDecision (lastDecision mkDecision) {
        decision = mkDecision;
    }

//    public static void main(String[] args) {
//        DRSzkHandler.newClient("10.21.50.20",2181, 6000, 6000, 1000, 3).start();
//        NodeCache nodeCache = null;
//        try {
//            nodeCache = DRSzkHandler.createNodeCache("/storm");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
////        System.out.println(s.getCurrentData().toString());
//        System.out.println(nodeCache.getClass());
//        System.out.println(nodeCache.getListenable().size());
//        nodeCache.getListenable().addListener(new NodeCacheListener() {
//
//            public void nodeChanged() throws Exception {
//                System.out.println(1);
//            }
//        }, DRSzkHandler.EXECUTOR_SERVICE);
//        nodeCache.getListenable().addListener(new NodeCacheListener() {
//
//            public void nodeChanged() throws Exception {
//                System.out.println(2);
//            }
//        }, DRSzkHandler.EXECUTOR_SERVICE);
//        System.out.println(nodeCache.getListenable().size());
//        System.out.println(DRSzkHandler.EXECUTOR_SERVICE.isShutdown());
//    }
}