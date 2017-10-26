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
import redis.clients.jedis.Jedis;
import resa.shedding.tools.AbstractSampler;
import resa.shedding.tools.ActiveSheddingSampler;
import resa.shedding.tools.DRSzkHandler;
import resa.shedding.tools.TestRedis;
import resa.topology.DelegatedSpout;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by kailin on 17/5/17.
 */
public class DefaultSheddableSpout extends DelegatedSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSheddableSpout.class);

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
    private AbstractSampler activeSheddingSampler;
    private int pendingMax;
    private double pendingThreshold;
    private transient CuratorFramework client;
    private boolean enablePassiveShedding;
    private boolean enableActiveShedding;
    private int dropCase = 0;
    private int interval;
    //private NodeCache nodeCache;
    private int taskId = 0;


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

    private class SheddingMeasurableMsgId {
        final String stream;
        public final Object msgId;
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
            DefaultSheddableSpout.SheddingMeasurableMsgId streamMsgId = (DefaultSheddableSpout.SheddingMeasurableMsgId) info.messageId;
            if (streamMsgId != null && streamMsgId. isSampled()) {
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
            DefaultSheddableSpout.SheddingMeasurableMsgId streamMsgId = (DefaultSheddableSpout.SheddingMeasurableMsgId) info.messageId;
            failureCountMetric.scope("failure").incr();
            //failureCountMetric.scope("failLatencyMs").incrBy(info.failLatencyMs);
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

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        taskId = context.getThisTaskId();
        enablePassiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.PASSIVE_SHEDDING_ENABLE, true);
        enableActiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.ACTIVE_SHEDDING_ENABLE, true);
        if (!enablePassiveShedding && !enableActiveShedding) {
            dropCase = 0;
            LOG.info("application running under no spout drop status: "+dropCase);
        } else if (enablePassiveShedding && enableActiveShedding) {
            dropCase = 1;
            LOG.info("application running under active spout drop and passive spout drop status: "+dropCase);
        } else if (enablePassiveShedding && !enableActiveShedding) {
            dropCase = 2;
            LOG.info("application running under passive spout drop status: "+dropCase);
        } else if (!enablePassiveShedding && enableActiveShedding) {
            dropCase = 3;
            LOG.info("application running under active spout drop status: "+dropCase);
        }
//        dropCase = (!enablePassiveShedding && !enableActiveShedding) ? 0
//                : ((enablePassiveShedding && enableActiveShedding) ? 1
//                : ((enablePassiveShedding && !enableActiveShedding) ? 2 : 3));
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
        context.addTaskHook(new SpoutHook());
        if (enableActiveShedding) {
            compID = context.getThisComponentId();
            topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
            client = DRSzkHandler.newClient(zkServer.get(0).toString(), 2181, 6000, 6000, 1000, 3);
            try {
                if (!DRSzkHandler.clientIsStart()) {
                    DRSzkHandler.start();
                }
                activeSheddingRate = (double) conf.get("test.shedding.rate2");
                System.out.println("haitianshengyan"+activeSheddingRate);
                activeSheddingSampler = new ActiveSheddingSampler(activeSheddingRate);
                watchActiveShedRatio();
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
                                taskId++;
                                if (taskId == 1) System.out.println("renew a taskIDD!");
                                //if (taskId > 1000)
                                    //System.out.println(activeSheddingSampler.getSampleValue()+"~~!~~"+activeSheddingRate+"number of active shedding:"+taskId+"~"+pendingCount);
                                failureCountMetric.scope("activeSpoutDrop").incr();
                                return null;
                            }
                            return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                        } else {
                           // System.out.println(taskId+"qiqi passive shedding start!"+pendingCount);
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
        super.ack(getUserMsgId(msgId));
    }

    @Override
    public void fail(Object msgId) {
        super.fail(getUserMsgId(msgId));
    }

    @Override
    public void nextTuple() {
        super.nextTuple();
    }

    public void watchActiveShedRatio() throws Exception {
        //if (DRSzkHandler.clientIsStart()) {
        NodeCache nodeCache = DRSzkHandler.createNodeCache("/drs/"+topologyName);
        LOG.info(compID+" watch active shedding ratio!");

            nodeCache.getListenable().addListener(new NodeCacheListener() {

                public void nodeChanged() throws Exception {
                    double shedRate = DRSzkHandler.parseActiveShedRateMap(nodeCache.getCurrentData().getData(), compID);
                    if (shedRate != Double.MAX_VALUE && shedRate != activeSheddingRate) {
                        System.out.println(activeSheddingRate + "nimabi" + compID + ":" + "hehe" + shedRate);
                        activeSheddingRate = shedRate;
                        activeSheddingSampler = new ActiveSheddingSampler(activeSheddingRate);
                        taskId = 0;
                    }
                }
            }, DRSzkHandler.EXECUTOR_SERVICE);
     //   }
    }
}
