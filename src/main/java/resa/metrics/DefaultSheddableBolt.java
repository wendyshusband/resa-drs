package resa.metrics;

import org.apache.storm.Config;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.shedding.basicServices.IShedding;
import resa.topology.DelegatedBolt;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kailin on 4/3/17.
 */


public final class DefaultSheddableBolt extends DelegatedBolt implements IShedding {
    public static Logger LOG = LoggerFactory.getLogger(DefaultSheddableBolt.class);
    private static final long serialVersionUID = 1L;
    private int tupleQueueCapacity;
    private transient BlockingQueue<Tuple> pendingTupleQueue;
    private transient BlockingQueue<Tuple> failTupleQueue;
    private double passiveSheddingThreshold;
    private transient MultiCountMetric passiveSheddingRateMetric;
    private HashMap<String,List<String>> activeSheddingStreamMap;
    private double activeSheddingRate;
    private String compID;
    private String topologyName;
    private Sampler activeSheddingSampler;
    private boolean ackFlag;

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
    //drs
    private transient CMVMetric executeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private transient DefaultSheddableBolt.SheddindMeasurableOutputCollector sheddindMeasurableCollector;
    private long lastMetricsSent;
    //private CuratorFramework client;
    private int interval;

    public DefaultSheddableBolt() {
    }

    public DefaultSheddableBolt(IRichBolt bolt){
        super(bolt);
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        executeMetric = context.registerMetric(MetricNames.TASK_EXECUTE, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        passiveSheddingRateMetric = context.registerMetric(MetricNames.PASSIVE_SHEDDING_RATE, new MultiCountMetric(),interval);
        lastMetricsSent = System.currentTimeMillis();
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        tupleQueueCapacity = ConfigUtil.getInt(conf,ResaConfig.TUPLE_QUEUE_CAPACITY,1024);
        passiveSheddingThreshold = ConfigUtil.getDouble(conf,ResaConfig.SHEDDING_THRESHOLD,0.8);
        sheddindMeasurableCollector = new DefaultSheddableBolt.SheddindMeasurableOutputCollector(outputCollector);
        super.prepare(conf, context, sheddindMeasurableCollector);
        pendingTupleQueue = new ArrayBlockingQueue<>(tupleQueueCapacity);
        //Config.TOPOLOGY_ACKER_EXECUTORS
        compID = context.getThisComponentId();
        topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        activeSheddingRate = 0.0;
        activeSheddingSampler = new Sampler(activeSheddingRate);
        ackFlag = Utils.getBoolean(conf.get("resa.ack.flag"),true);
        //List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        //int port = Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_PORT));
        //client= DRSzkHandler.newClient(zkServer.get(0).toString(),2181,6000,6000,1000,3);
        JSONParser parser = new JSONParser();
        try {
            activeSheddingStreamMap = (JSONObject) parser.parse(ConfigUtil.getString(conf,ResaConfig.ACTIVE_SHEDDING_MAP,"{}"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(ackFlag) {
            failTupleQueue = new ArrayBlockingQueue<>((tupleQueueCapacity*10));
            handlePassiveLoadSheddingFailTupleThread();
        }
        //checkActiveSheddingRateThread();
        handleTupleThread();
        LOG.info("Preparing DefaultSheddableBolt: " + context.getThisComponentId());
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

//    private void checkActiveSheddingRateThread() {
//        final Thread thread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while(true){
//                    if(!client.isStarted())
//                        DRSzkHandler.start();
//                    try {
//                        if(null != client.checkExists().forPath("/drs/"+topologyName)){
//                            String tempMap = new String(client.getData().forPath("/drs/"+topologyName));
//                            //Double rate = tempMap
//                            Pattern pattern1 = Pattern.compile(compID+"=(\\d+)\\.(\\d+)");
//                            Matcher matcher1 = pattern1.matcher(tempMap);
//                            if(matcher1.find()) {
//                                Pattern pattern2 = Pattern.compile("(\\d+)\\.(\\d+)");
//                                Matcher matcher2 = pattern2.matcher(matcher1.group());
//                                if(matcher2.find()) {
//                                    double shedRate = Double.valueOf(matcher2.group());
//                                    if (shedRate != activeSheddingRate) {
//                                        //LOG.info(activeSheddingRate+"woshenzhiyouyi"+compID+":"+"hehe"+shedRate);
//                                        activeSheddingRate = shedRate;
//                                        activeSheddingSampler = new Sampler(activeSheddingRate);
//                                        //LOG.info(activeSheddingSampler.toString() + "tabu");
//                                    }
//                                }
//                            }
//                        }
//                        Thread.sleep(interval);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//
//                }
//            }
//        });
//        thread.start();
//        LOG.info("checkActiveSheddingRateThread thread start!");
//    }

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

    public void execute(Tuple tuple) {
        boolean flag = true;
        passiveSheddingRateMetric.scope("allTuple").incr();
        if (trigger(null)){// need passive shedding
            int sheddTupleNum =passiveDrop(null);
            passiveSheddingRateMetric.scope("dropTuple").incrBy(sheddTupleNum);
            passiveSheddingRateMetric.scope("dropFrequency").incr();
        }else {// do not need passive shedding
            passiveSheddingRateMetric.scope("dropTuple").incrBy(0);
            passiveSheddingRateMetric.scope("dropFrequency").incrBy(0);
//            if(activeSheddingRate != 0.0) {
//                if (activeSheddingStreamMap.containsKey(tuple.getSourceComponent())) {
//                    if (activeSheddingStreamMap.get(tuple.getSourceComponent()).contains(tuple.getSourceStreamId())) {
//                        //LOG.info(compID + " : " + activeSheddingRate + "wobu "+tuple.getSourceStreamId());
//                        if (activeSheddingSampler.shoudSample()) {
//                            flag = false;
//                        }
//                    }
//                }
//            }
        }

        try {
            if(flag) {
                pendingTupleQueue.put(tuple);
            }else{

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int passiveDrop(Object[] arg) {
        int sheddTupleNum = pendingTupleQueue.size() * pendingTupleQueue.size() / tupleQueueCapacity;
        List tempList = new LinkedList();
        pendingTupleQueue.drainTo(tempList,sheddTupleNum);
        if(ackFlag){
            failTupleQueue.addAll(tempList);
        }else{
            tempList.clear();
        }
        return sheddTupleNum;
    }

    @Override
    public boolean trigger(Object[] arg) {
        return (pendingTupleQueue.size() >= (passiveSheddingThreshold * tupleQueueCapacity));
    }

}

