package resa.shedding.example;

import org.apache.storm.Config;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.CMVMetric;
import resa.metrics.MetricNames;
import resa.shedding.basicServices.api.IShedding;
import resa.topology.DelegatedBolt;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kailin on 2017/6/25.
 */
public class SimpleREDsheddableBolt extends DelegatedBolt implements IShedding {
    public static Logger LOG = LoggerFactory.getLogger(SimpleREDsheddableBolt.class);
    private int tupleQueueCapacity;
    private transient BlockingQueue<Tuple> pendingTupleQueue;
    private transient BlockingQueue<Tuple> failTupleQueue;
    private double passiveSheddingHighThreshold;
    private double passiveSheddingLowThreshold;
    private double maxPassiveShedRate;
    private transient MultiCountMetric passiveSheddingRateMetric;
    private String compID;
    private String topologyName;
    private boolean ackFlag;
    //drs
    private transient CMVMetric executeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private transient SheddindMeasurableOutputCollector sheddindMeasurableCollector;
    private long lastMetricsSent;
    private int interval;

    public SimpleREDsheddableBolt() {
    }
    public SimpleREDsheddableBolt(IRichBolt bolt){
        super(bolt);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        interval = Utils.getInt(stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        executeMetric = context.registerMetric(MetricNames.TASK_EXECUTE, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        passiveSheddingRateMetric = context.registerMetric(MetricNames.SHEDDING_RATE, new MultiCountMetric(),interval);
        lastMetricsSent = System.currentTimeMillis();
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);
        sampler = new Sampler(ConfigUtil.getDouble(stormConf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        tupleQueueCapacity = ConfigUtil.getInt(stormConf,ResaConfig.TUPLE_QUEUE_CAPACITY,1024);
        passiveSheddingHighThreshold = ConfigUtil.getDouble(stormConf,ResaConfig.HIGH_SHEDDING_THRESHOLD,0.95);
        passiveSheddingLowThreshold = ConfigUtil.getDouble(stormConf,ResaConfig.LOW_SHEDDING_THRESHOLD,0.75);
        maxPassiveShedRate = ConfigUtil.getDouble(stormConf,ResaConfig.MAX_SHED_RATE,0.1);
        sheddindMeasurableCollector = new SheddindMeasurableOutputCollector(outputCollector);
        super.prepare(stormConf, context, sheddindMeasurableCollector);
        pendingTupleQueue = new ArrayBlockingQueue<>(tupleQueueCapacity);
        compID = context.getThisComponentId();
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        ackFlag = Utils.getBoolean(stormConf.get("resa.ack.flag"),true);
        if(ackFlag) {
            failTupleQueue = new ArrayBlockingQueue<>((tupleQueueCapacity*10));
            handlePassiveLoadSheddingFailTupleThread();
        }
        handleTupleThread();
        passiveSheddingRateMetric.scope("allTuple").incrBy(0);
        passiveSheddingRateMetric.scope("dropTuple").incrBy(0);
        passiveSheddingRateMetric.scope("dropFrequency").incrBy(0);
        LOG.info("Preparing SimpleREDsheddableBolt: " + context.getThisComponentId());
    }

    private long getMetricsDuration() {
        long now = System.currentTimeMillis();
        long duration = now - lastMetricsSent;
        lastMetricsSent = now;
        return duration;
    }

    private void handlePassiveLoadSheddingFailTupleThread() {
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Tuple failTuple = null;
                while (true){
                    try {
                        failTuple = failTupleQueue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    sheddindMeasurableCollector.fail(failTuple);
                }
            }
        });
        thread.start();
        LOG.info("handlePassiveLoadSheddingFailTupleThread thread start!");
    }

    private void handleTupleThread() {
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                boolean done = false;
                Tuple tuple = null;
                while (!done){
                    try {
                        tuple = pendingTupleQueue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    handle(tuple);
                }
            }
        });
        thread.start();
        LOG.info("handleTupleThread thread start!");
    }

    private void handle(Tuple tuple) {
        long elapse;
        if (sampler.shoudSample()) {
            // enable emit sample
            sheddindMeasurableCollector.setEmitSample(true);
            long arrivalTime = System.nanoTime();
            super.execute(tuple);
            elapse = System.nanoTime() - arrivalTime;
        } else {
            elapse = -1;
            // disable emit sample
            sheddindMeasurableCollector.setEmitSample(false);
            super.execute(tuple);
        }
        // avoid numerical overflow
        if (elapse > 0) {
            String id = tuple.getSourceComponent() + ":" + tuple.getSourceStreamId();
            executeMetric.addMetric(id, elapse / 1000000.0);
        }
    }

    @Override
    public void execute(Tuple input) {
        passiveSheddingRateMetric.scope("allTuple").incr();
        if (trigger(null)){
            int shedTupleNum =passiveDrop(null);
            passiveSheddingRateMetric.scope("dropTuple").incrBy(shedTupleNum);
            passiveSheddingRateMetric.scope("dropFrequency").incr();
        }
        try {
            pendingTupleQueue.put(input);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public int passiveDrop(Object[] arg) {
        int shedTupleNum;
        int tupleNums = pendingTupleQueue.size();
        List tempList = new LinkedList();
        if (tupleNums >= (passiveSheddingHighThreshold * tupleQueueCapacity)){
            shedTupleNum = (int) (tupleNums * passiveSheddingHighThreshold);
        } else {
            shedTupleNum = (int) (tupleNums - (passiveSheddingLowThreshold * tupleQueueCapacity))*2;
        }

//        int sheddTupleNum;
//        int tupleNums = pendingTupleQueue.size();
//        double currentPenddingRate = (tupleNums * 1.0)/ tupleQueueCapacity;
//        List tempList = new LinkedList();
//        if (tupleNums >= (passiveSheddingHighThreshold * tupleQueueCapacity)){
//            sheddTupleNum = (int) (tupleNums * currentPenddingRate);
//        } else {
//            sheddTupleNum = (int) (tupleNums * (maxPassiveShedRate * (currentPenddingRate
//                    - passiveSheddingLowThreshold) / (passiveSheddingHighThreshold - passiveSheddingLowThreshold)));
//        }
        System.out.println(tupleNums+"~"+"RED sheddTuple: "+shedTupleNum);
        //System.out.println(tupleNums+"~"+currentPenddingRate+"RED sheddTuple: "+shedTupleNum);
        pendingTupleQueue.drainTo(tempList,shedTupleNum);
        failTupleQueue.addAll(tempList);
        return shedTupleNum;
    }

    @Override
    public boolean trigger(Object[] arg) {
        return (pendingTupleQueue.size() >= (passiveSheddingLowThreshold * tupleQueueCapacity));
    }

    private class SheddindMeasurableOutputCollector extends OutputCollector {

        private boolean sample = false;

        SheddindMeasurableOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        public void setEmitSample(boolean sample) {
            this.sample = sample;
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            if (sample) {
                emitMetric.scope(streamId).incr();
            }
            return super.emit(streamId, anchors, tuple);
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            if (sample) {
                emitMetric.scope(streamId).incr();
            }
            super.emitDirect(taskId, streamId, anchors, tuple);
        }

    }
}
