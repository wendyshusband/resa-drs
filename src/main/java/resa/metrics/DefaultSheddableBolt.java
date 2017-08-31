package resa.metrics;

import org.apache.storm.Config;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.storm.shade.org.apache.curator.framework.recipes.cache.NodeCacheListener;
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
import resa.shedding.basicServices.api.IShedding;
import resa.shedding.tools.AbstractSampler;
import resa.shedding.tools.ActiveSheddingSampler;
import resa.shedding.tools.DRSzkHandler;
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
    private transient MultiCountMetric sheddingRateMetric;
    private HashMap<String,List<String>> activeSheddingStreamMap;
    private double activeSheddingRate;
    private String compID;
    private String topologyName;
    private AbstractSampler activeSheddingSampler;
    //private boolean ackFlag;
    private boolean enablePassiveShedding;
    private boolean enableActiveShedding;
    private int sheddingCase = 0;

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
    private transient CuratorFramework client;
    private int interval;

    public DefaultSheddableBolt() {
    }

    public DefaultSheddableBolt(IRichBolt bolt){
        super(bolt);
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        enablePassiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.PASSIVE_SHEDDING_ENABLE, true);
        enableActiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.ACTIVE_SHEDDING_ENABLE, true);
        if (!enablePassiveShedding && !enableActiveShedding) {
            sheddingCase = 0;
            LOG.info("application running under no shedding status: "+sheddingCase);
        } else if (enablePassiveShedding && enableActiveShedding) {
            sheddingCase = 1;
            LOG.info("application running under active shedding and passive shedding status: "+sheddingCase);
        } else if (enablePassiveShedding && !enableActiveShedding) {
            sheddingCase = 2;
            LOG.info("application running under passive shedding status: "+sheddingCase);
        } else if (!enablePassiveShedding && enableActiveShedding) {
            sheddingCase = 3;
            LOG.info("application running under active shedding status: "+sheddingCase);
        }
        interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);
        executeMetric = context.registerMetric(MetricNames.TASK_EXECUTE, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        sheddingRateMetric = context.registerMetric(MetricNames.SHEDDING_RATE, new MultiCountMetric(),interval);

        lastMetricsSent = System.currentTimeMillis();
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        tupleQueueCapacity = ConfigUtil.getInt(conf,ResaConfig.TUPLE_QUEUE_CAPACITY,1024);
        passiveSheddingThreshold = ConfigUtil.getDouble(conf,ResaConfig.SHEDDING_THRESHOLD,0.8);

        sheddindMeasurableCollector = new SheddindMeasurableOutputCollector(outputCollector);
        super.prepare(conf, context, sheddindMeasurableCollector);
        pendingTupleQueue = new ArrayBlockingQueue<>(tupleQueueCapacity);

        if (enableActiveShedding) {
            compID = context.getThisComponentId();
            topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            //ackFlag = Utils.getBoolean(conf.get("resa.ack.flag"),true);
            List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
            int port = Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_PORT));
            client= DRSzkHandler.newClient(zkServer.get(0).toString(),port,6000,6000,1000,3);
            JSONParser parser = new JSONParser();
            try {
                if (!DRSzkHandler.clientIsStart()) {
                    DRSzkHandler.start();
                }
                activeSheddingRate = 0.0;
                activeSheddingSampler = new ActiveSheddingSampler(activeSheddingRate);
                activeSheddingStreamMap = (JSONObject) parser.parse(ConfigUtil.getString(conf, ResaConfig.ACTIVE_SHEDDING_MAP, "{}"));
                watchActiveShedRate();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            //checkActiveSheddingRateThread();
        }

        //if(ackFlag) {
        if (sheddingCase != 0) {
            failTupleQueue = new ArrayBlockingQueue<>((tupleQueueCapacity * 10));
            handleLoadSheddingFailTupleThread();
        }
        //}
        handleTupleThread();
        sheddingRateMetric.scope("dropTuple").incrBy(0);
        sheddingRateMetric.scope("dropFrequency").incrBy(0);
        sheddingRateMetric.scope("allTuple").incrBy(0);
        sheddingRateMetric.scope("activeDrop").incrBy(0);

        LOG.info("Preparing DefaultSheddableBolt: " + context.getThisComponentId());
    }

    private long getMetricsDuration() {
        long now = System.currentTimeMillis();
        long duration = now - lastMetricsSent;
        lastMetricsSent = now;
        return duration;
    }

    private void handleLoadSheddingFailTupleThread() {
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
//                    if(!DRSzkHandler.clientIsStart())
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
        sheddingRateMetric.scope("allTuple").incr();
        boolean flag = true;
        switch (sheddingCase) {
            case 0: break;
            case 1: {
                if (trigger(null)){// need passive shedding
                    int sheddTupleNum =passiveDrop(null);
                    sheddingRateMetric.scope("dropTuple").incrBy(sheddTupleNum);
                    sheddingRateMetric.scope("dropFrequency").incr();
                } else {
                    if(activeSheddingRate != 0.0) {
                        if (activeSheddingStreamMap.containsKey(tuple.getSourceComponent())) {
                            if (activeSheddingStreamMap.get(tuple.getSourceComponent()).contains(tuple.getSourceStreamId())) {
                                //LOG.info(compID + " : " + activeSheddingRate + "wobu "+tuple.getSourceStreamId());
                                if (activeSheddingSampler.shoudSample()) {
                                    flag = false;
                                }
                            }
                        }
                    }
                }
                break;
            }
            case 2: {
                if (trigger(null)){// need passive shedding
                    int sheddTupleNum =passiveDrop(null);
                    sheddingRateMetric.scope("dropTuple").incrBy(sheddTupleNum);
                    sheddingRateMetric.scope("dropFrequency").incr();
                }
                break;
            }
            case 3: {
                if(activeSheddingRate != 0.0) {
                    if (activeSheddingStreamMap.containsKey(tuple.getSourceComponent())) {
                        if (activeSheddingStreamMap.get(tuple.getSourceComponent()).contains(tuple.getSourceStreamId())) {
                            //LOG.info(compID + " : " + activeSheddingRate + "wobu "+tuple.getSourceStreamId());
                            if (activeSheddingSampler.shoudSample()) {
                                flag = false;
                            }
                        }
                    }
                }
                break;
            }
            default: {
                LOG.error("bad sheddingCase. start no shedding bolt");
            }
        }
        try {
            if(flag) {
                pendingTupleQueue.put(tuple);
            }else{
                failTupleQueue.put(tuple);
                sheddingRateMetric.scope("activeDrop").incr();
                sheddingRateMetric.scope("dropTuple").incr();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void watchActiveShedRate() throws Exception {
        NodeCache nodeCache = DRSzkHandler.createNodeCache("/drs/"+topologyName);
        LOG.info(compID+" watch active shedding ratio!");
        //if (DRSzkHandler.clientIsStart()) {

            nodeCache.getListenable().addListener(new NodeCacheListener() {

                public void nodeChanged() throws Exception {
                    double shedRate = DRSzkHandler.parseActiveShedRateMap(nodeCache.getCurrentData().getData(), compID);
                    System.out.println(shedRate+"weipinhuihuihuihuihui "+compID);
                    if (shedRate != Double.MAX_VALUE && shedRate != activeSheddingRate) {
                        System.out.println(activeSheddingRate + "womabi" + compID + ":" + "haha" + shedRate);
                        activeSheddingRate = shedRate;
                        activeSheddingSampler = new ActiveSheddingSampler(activeSheddingRate);
                    }
                }
            }, DRSzkHandler.EXECUTOR_SERVICE);
        //}
    }

    @Override
    public int passiveDrop(Object[] arg) {
        int sheddTupleNum = pendingTupleQueue.size() * pendingTupleQueue.size() / tupleQueueCapacity;
        List tempList = new LinkedList();
        pendingTupleQueue.drainTo(tempList,sheddTupleNum);
        //if(ackFlag){
        failTupleQueue.addAll(tempList);
//        }else{
//            tempList.clear();
//        }
        return sheddTupleNum;
    }

    @Override
    public boolean trigger(Object[] arg) {
        return (pendingTupleQueue.size() >= (passiveSheddingThreshold * tupleQueueCapacity));
    }

}

