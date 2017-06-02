package resa.metrics;

import org.apache.storm.Config;
import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private int spoutMaxPending;
    //private boolean ackFlag;
    private transient MultiCountMetric failureCountMetric;
    private double activeSheddingRate;
    private String compID;
    private String topologyName;
    private Sampler activeSheddingSampler;
    //private CuratorFramework client;

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
        //spoutMaxPending = Integer.valueOf(Utils.getString(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)));
        //ackFlag = Utils.getBoolean(conf.get("resa.ack.flag"),false);
        int interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        failureCountMetric = context.registerMetric(MetricNames.FAILURE_COUNT,new MultiCountMetric(),interval);
        failureCountMetric.scope("failure").incrBy(0);
        failureCountMetric.scope("spoutDrop").incrBy(0);
        compID = context.getThisComponentId();
        topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        activeSheddingRate = 0.0;
        activeSheddingSampler = new Sampler(activeSheddingRate);
//        List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
//        client= DRSzkHandler.newClient(zkServer.get(0).toString(),2181,6000,6000,1000,3);
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
        super.open(conf, context, new SpoutOutputCollector(collector) {

            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                if(messageId != null) {//ack
                    //LOG.info(pendingCount+"jian"+collector.getPendingCount());
                    pendingCount = (int) collector.getPendingCount();
                    if(pendingCount <= 0.9*1024) {
                        return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
                    }
                    else {
                        failureCountMetric.scope("spoutDrop").incr();
                        return null;
                    }
                }
                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                if(messageId != null) {//ack
                    pendingCount = (int) collector.getPendingCount();
                    if(pendingCount <= 0.9*1024) {
                        super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
                    }else{
                        failureCountMetric.scope("spoutDrop").incr();
                    }
                }else{// no ack
                    super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
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
            //LOG.info(pendingCount+"jianbujian");
    }

  /*  private void checkActiveSheddingRateThread() {
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    if(!client.isStarted())
                        client.start();
                    try {
                        if(null != client.checkExists().forPath("/drs/"+topologyName)){
                            String tempMap = new String(client.getData().forPath("/drs/"+topologyName));
                            Pattern pattern1 = Pattern.compile(compID+"=(\\d+)\\.(\\d+)");
                            Matcher matcher1 = pattern1.matcher(tempMap);
                            if(matcher1.find()) {
                                Pattern pattern2 = Pattern.compile("(\\d+)\\.(\\d+)");
                                Matcher matcher2 = pattern2.matcher(matcher1.group());
                                if(matcher2.find()) {
                                    double shedRate = Double.valueOf(matcher2.group());
                                    if (shedRate != activeSheddingRate) {
                                        //LOG.info(activeSheddingRate+"woshenzhiyouyi"+compID+":"+"hehe"+shedRate);
                                        activeSheddingRate = shedRate;
                                        activeSheddingSampler = new Sampler(activeSheddingRate);
                                        //LOG.info(activeSheddingSampler.toString() + "tabu");
                                    }
                                }
                            }
                        }
                        Thread.sleep(interval);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        });
        thread.start();
        LOG.info("checkActiveSheddingRateThread thread start!");
    }*/
}
