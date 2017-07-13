package resa.metrics;

import org.apache.storm.Config;
import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.storm.shade.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.shedding.tools.ActiveSheddingSampler;
import resa.shedding.tools.DRSzkHandler;
import resa.topology.DelegatedSpout;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Created by kailin on 17/5/17.
 */
public class DefaultSheddableSpout extends DelegatedSpout{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSheddableSpout.class);
    private class SheddingMeasurableMsgId {
        final String stream;
        final Object msgId;
        final long startTime;

        private SheddingMeasurableMsgId(String stream, Object msgId, long startTime) {
            this.stream = stream;
            this.msgId = msgId;
            this.startTime = startTime;
        }

        public boolean isSampled() {
            return startTime > 0;
        }
    }

    private class SpoutHook extends BaseTaskHook {

        @Override
        public void spoutAck(SpoutAckInfo info) {
            SheddingMeasurableMsgId streamMsgId = (SheddingMeasurableMsgId) info.messageId;
            if (streamMsgId != null && streamMsgId.isSampled()) {
                long cost = System.currentTimeMillis() - streamMsgId.startTime;
                completeMetric.addMetric(streamMsgId.stream, cost);
                if (cost > qos) {
                    missMetric.addMetric(streamMsgId.stream, cost);
                }
                if (completeStatMetric != null) {
                    completeStatMetric.add(streamMsgId.stream, cost);
                }
            }
        }

        @Override
        public void spoutFail(SpoutFailInfo info) {
            SheddingMeasurableMsgId streamMsgId = (SheddingMeasurableMsgId) info.messageId;
            failureCountMetric.scope("failure").incr();
            failureCountMetric.scope("failLatencyMs").incrBy(info.failLatencyMs);
            if (streamMsgId != null && streamMsgId.isSampled()) {
                if (completeStatMetric != null) {
                    completeStatMetric.fail(streamMsgId.stream);
                }
            }
        }

        @Override
        public void emit(EmitInfo info) {
            //failureCountMetric.scope("spoutEmit").incr();
            super.emit(info);
        }
    }

    private transient CMVMetric completeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private transient CMVMetric missMetric;
    private transient CompleteStatMetric completeStatMetric;
    private long lastMetricsSent;
    private long qos;
    private int pendingCount = 0;
    private transient MultiCountMetric failureCountMetric;
    private double activeSheddingRate;
    private String compID;
    private String topologyName;
    private ActiveSheddingSampler activeSheddingSampler;
    private int pendingMax;
    private double pendingThreshold;
    private transient CuratorFramework client;
    private boolean enablePassiveShedding;
    private boolean enableActiveShedding;
    private int dropCase = 0;
    private int interval;
    private String tempStr;// = new String[1];

    public DefaultSheddableSpout(){

    }
    public DefaultSheddableSpout(IRichSpout delegate) {
        super(delegate);
    }

    private long getMetricsDuration() {
        long now = System.currentTimeMillis();
        long duration = now - lastMetricsSent;
        lastMetricsSent = now;
        return duration;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        enablePassiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.PASSIVE_SHEDDING_ENABLE, true);
        enableActiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.ACTIVE_SHEDDING_ENABLE, true);
        if (enablePassiveShedding == false && enableActiveShedding == false) {
            dropCase = 0;
            LOG.info("application running under no spout drop status: "+dropCase);
        } else if (enablePassiveShedding == true && enableActiveShedding == true) {
            dropCase = 1;
            LOG.info("application running under active spout drop and passive spout drop status: "+dropCase);
        } else if (enablePassiveShedding == true && enableActiveShedding == false) {
            dropCase = 2;
            LOG.info("application running under passive spout drop status: "+dropCase);
        } else if (enablePassiveShedding == false && enableActiveShedding == true) {
            dropCase = 3;
            LOG.info("application running under active spout drop status: "+dropCase);
        }

        interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        failureCountMetric = context.registerMetric(MetricNames.FAILURE_COUNT,new MultiCountMetric(),interval);
        completeMetric = context.registerMetric(MetricNames.COMPLETE_LATENCY, new CMVMetric(), interval);
        // register miss metric
        qos = ConfigUtil.getLong(conf, "resa.metric.complete-latency.threshold.ms", Long.MAX_VALUE);
        missMetric = context.registerMetric(MetricNames.MISS_QOS, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        // register stat metric
        double[] xAxis = Stream.of(((String) conf.getOrDefault("resa.metric.complete-latency.stat.x-axis", ""))
                .split(",")).filter(s -> !s.isEmpty()).mapToDouble(Double::parseDouble).toArray();
        completeStatMetric = xAxis.length > 0 ? context.registerMetric(MetricNames.LATENCY_STAT,
                new CompleteStatMetric(xAxis), interval) : null;
        // register duration metric
        lastMetricsSent = System.currentTimeMillis();
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        context.addTaskHook(new DefaultSheddableSpout.SpoutHook());
        if (enableActiveShedding) {
            compID = context.getThisComponentId();
            topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
            client = DRSzkHandler.newClient(zkServer.get(0).toString(), 2181, 6000, 6000, 1000, 3);
            //checkActiveSheddingRateThread();

                try {
                    if (!client.isStarted()) {
                        client.start();
                    }
                    if (client.isStarted() && client.checkExists().forPath("/drs/" + topologyName) != null) {
                        activeSheddingRate = parseActiveShedRateMap(client.getData().forPath("/drs/" + topologyName)) > 1 ? 0.0 : parseActiveShedRateMap(client.getData().forPath("/drs/" + topologyName));
                    } else {
                        activeSheddingRate = 0.0;
                    }

                    System.out.println("chushihuade rate:"+activeSheddingRate);
                    activeSheddingSampler = new ActiveSheddingSampler(activeSheddingRate);
                    watchChild("/drs/"+topologyName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }

        pendingMax = ConfigUtil.getInt(conf, ResaConfig.SPOUT_MAX_PENDING, 1024);
        pendingThreshold =ConfigUtil.getDouble(conf,ResaConfig.SPOUT_PENDING_THRESHOLD,0.8);

        failureCountMetric.scope("failure").incrBy(0);
        failureCountMetric.scope("spoutDrop").incrBy(0);
        failureCountMetric.scope("failLatencyMs").incrBy(0);
        failureCountMetric.scope("activeSpoutDrop").incrBy(0);

        super.open(conf, context, new SpoutOutputCollector(collector) {

            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                switch (dropCase) {
                    case 0: {
                        return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                    }
                    case 1: {
                        pendingCount = (int) collector.getPendingCount();
                        if(pendingCount <= (pendingThreshold * pendingMax)){
                            if(activeSheddingRate != 0.0 && activeSheddingSampler.shoudSample()){
                                failureCountMetric.scope("spoutDrop").incr();
                                failureCountMetric.scope("activeSpoutDrop").incr();
                                return null;
                            }
                            return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                        } else {
                            failureCountMetric.scope("spoutDrop").incr();
                            return null;
                        }
                    }
                    case 2: {
                        if(messageId != null) {//ack
                            pendingCount = (int) collector.getPendingCount();
                            if(pendingCount <= (pendingThreshold * pendingMax)) {
                                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                            } else {
                                failureCountMetric.scope("spoutDrop").incr();
                                return null;
                            }
                        }
                        return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                    }
                    case 3: {
                        if(activeSheddingRate != 0.0 && activeSheddingSampler.shoudSample()) {
                            failureCountMetric.scope("spoutDrop").incr();
                            failureCountMetric.scope("activeSpoutDrop").incr();
                            return null;
                        }
                        return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                    }
                    default: {
                        LOG.error("bad dropCase. start no drop spout ");
                    }
                }
                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {

                switch (dropCase) {
                    case 0: {
                        super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                        break;
                    }
                    case 1: {
                        pendingCount = (int) collector.getPendingCount();
                        if (pendingCount <= (pendingThreshold * pendingMax)) {
                            if (activeSheddingRate == 0.0 || !activeSheddingSampler.shoudSample()) {
                                super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                            } else {
                                failureCountMetric.scope("spoutDrop").incr();
                                failureCountMetric.scope("activeSpoutDrop").incr();
                            }
                        } else {
                            failureCountMetric.scope("spoutDrop").incr();
                        }
                    }
                    case 2: {
                        if (messageId != null) {//ack
                            pendingCount = (int) collector.getPendingCount();
                            if(pendingCount <= (pendingThreshold * pendingMax)) {
                                super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
                            }else{
                                failureCountMetric.scope("spoutDrop").incr();
                            }
                        } else {// no ack
                            super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
                        }
                    }
                    case 3: {
                        if(activeSheddingRate == 0.0 || !activeSheddingSampler.shoudSample()) {
                            super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                        } else {
                            failureCountMetric.scope("spoutDrop").incr();
                            failureCountMetric.scope("activeSpoutDrop").incr();
                        }
                    }
                    default: {
                        LOG.error("bad dropCase. start no drop spout ");
                        super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                    }
                }
            }

            private SheddingMeasurableMsgId newStreamMessageId(String stream, Object messageId) {
                long startTime;
                if (sampler.shoudSample()) {
                    startTime = System.currentTimeMillis();
                    emitMetric.scope(stream).incr();
                } else {
                    startTime = -1;
                }
                return messageId == null ? null : new SheddingMeasurableMsgId(stream, messageId, startTime);
            }
        });

        LOG.info("Preparing DefaultSheddableSpout: " + context.getThisComponentId());
    }

    private Object getUserMsgId(Object msgId) {
        return msgId != null ? ((SheddingMeasurableMsgId) msgId).msgId : msgId;
    }

    @Override
    public void ack(Object msgId) {
//        if(pendingCount > 0)
//            pendingCount--;
//        LOG.info(pendingCount+"acklei");

        super.ack(getUserMsgId(msgId));
    }

    @Override
    public void fail(Object msgId) {
//        if(pendingCount> 0)
//            pendingCount--;
//        LOG.info(pendingCount+"faillei");
        super.fail(getUserMsgId(msgId));
    }

    @Override
    public void nextTuple() {
        super.nextTuple();
    }

    private double parseActiveShedRateMap(byte[] activeShedRateMapBytes) {
        String tempMap = new String(activeShedRateMapBytes);
        Pattern pattern1 = Pattern.compile(compID + "=(\\d+)\\.(\\d+)");
        Matcher matcher1 = pattern1.matcher(tempMap);
        if (matcher1.find()) {
            Pattern pattern2 = Pattern.compile("(\\d+)\\.(\\d+)");
            Matcher matcher2 = pattern2.matcher(matcher1.group());
            if (matcher2.find()) {
                return Double.valueOf(matcher2.group());
            }
        }
        return Double.MAX_VALUE;
    }

    public void watchChild(String path) throws Exception {
        System.out.println("fauckni");
        if (client.isStarted()) {
            System.out.println("fauckni1");
            DRSzkHandler.nodeCache = new NodeCache(client, path);
            DRSzkHandler.nodeCache.getListenable().addListener(new NodeCacheListener() {

                public void nodeChanged() throws Exception {
                    System.out.println(compID + "nimabi" + DRSzkHandler.nodeCache.getCurrentData().getPath() + ":" + new String(DRSzkHandler.nodeCache.getCurrentData().getData()));
                    double shedRate = parseActiveShedRateMap(DRSzkHandler.nodeCache.getCurrentData().getData());
                    if (shedRate != Double.MAX_VALUE) {
                        System.out.println(activeSheddingRate + "woshenzhiyouyi" + compID + ":" + "hehe" + shedRate);
                        activeSheddingRate = shedRate;
                        activeSheddingSampler = new ActiveSheddingSampler(activeSheddingRate);
                        System.out.println(activeSheddingSampler.toString() + "tabu");
                    }
                }
            }, DRSzkHandler.EXECUTOR_SERVICE);
            DRSzkHandler.nodeCache.start();
        }
    }
//    private void checkActiveSheddingRateThread() {
//        final Thread thread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while (true) {
//
//                    if (client.isStarted()) {
//                        try {
//
//                            if (null != tempMap) {
//                                String tempMap = new String(client.getData().forPath("/drs/" + topologyName));
//                                Pattern pattern1 = Pattern.compile(compID + "=(\\d+)\\.(\\d+)");
//                                Matcher matcher1 = pattern1.matcher(tempMap);
//                                if (matcher1.find()) {
//                                    Pattern pattern2 = Pattern.compile("(\\d+)\\.(\\d+)");
//                                    Matcher matcher2 = pattern2.matcher(matcher1.group());
//                                    if (matcher2.find()) {
//                                        double shedRate = Double.valueOf(matcher2.group());
//                                        if (shedRate != activeSheddingRate) {
//                                            //LOG.info(activeSheddingRate+"woshenzhiyouyi"+compID+":"+"hehe"+shedRate);
//                                            activeSheddingRate = shedRate;
//                                            activeSheddingSampler = new Sampler(activeSheddingRate);
//                                            //LOG.info(activeSheddingSampler.toString() + "tabu");
//                                        }
//                                    }
//                                }
//                            }
//                            Thread.sleep(interval * 500);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }
//        });
//        thread.start();
//        LOG.info("checkActiveSheddingRateThread thread start!");
//    }
}
