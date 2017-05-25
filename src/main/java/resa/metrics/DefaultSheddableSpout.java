package resa.metrics;

import org.apache.storm.Config;
import org.apache.storm.hooks.BaseTaskHook;
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
import java.util.concurrent.atomic.AtomicInteger;
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
            if (streamMsgId != null && streamMsgId.isSampled()) {
                if (completeStatMetric != null) {
                    completeStatMetric.fail(streamMsgId.stream);
                }
            }
        }
    }

    private transient CMVMetric completeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private transient CMVMetric missMetric;
    private transient CompleteStatMetric completeStatMetric;
    private long lastMetricsSent;
    private long qos;
    private AtomicInteger pendingCount = new AtomicInteger(0);
    private int spoutMaxPending;
    private boolean ackFlag;
    private transient MultiCountMetric failureCountMetric;

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
        spoutMaxPending = Integer.valueOf(Utils.getString(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)));
        //passiveSheddingTupleThread();
        ackFlag = Utils.getBoolean(conf.get("resa.ack.flag"),false);
        int interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        failureCountMetric = context.registerMetric(MetricNames.FAILURE_COUNT,new MultiCountMetric(),interval);
        failureCountMetric.scope("failure").incrBy(0);
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
                if(messageId != null)
                    pendingCount.getAndIncrement();
                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                if(messageId != null)
                    pendingCount.getAndIncrement();
                super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
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
        pendingCount.getAndDecrement();
        //LOG.info(pendingCount.get()+"ackyoho nexttuple!");
        super.ack(getUserMsgId(msgId));
    }

    @Override
    public void fail(Object msgId) {
        pendingCount.getAndDecrement();
        //LOG.info(pendingCount.get()+"failyoho nexttuple!");
        super.fail(getUserMsgId(msgId));
    }

    @Override
    public void nextTuple() {
        if(ackFlag){
            if(spoutMaxPending * 0.9 >= pendingCount.get()) {
                super.nextTuple();
                //LOG.info(pendingCount.get()+"yoho nexttuple!"+ackFlag);
            }
        }else{
            super.nextTuple();
            //LOG.info(pendingCount.get()+"yoha nexttuple!"+ackFlag);
        }

    }
}
