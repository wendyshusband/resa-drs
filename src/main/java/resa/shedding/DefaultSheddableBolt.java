package resa.shedding;

import org.apache.storm.Config;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.CMVMetric;
import resa.metrics.MeasurableBolt;
import resa.metrics.MetricNames;
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


public class DefaultSheddableBolt extends DelegatedBolt implements IShedding {
    public static Logger LOG = LoggerFactory.getLogger(DefaultSheddableBolt.class);

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

    //private IRichBolt _bolt;
    //private IShedding _shedder;
    //private transient OutputCollector _collector;
    final int tupleQueueCapacity = 10;
    private transient BlockingQueue<Tuple> pendingTupleQueue;

    //drs
    private transient CMVMetric executeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private transient DefaultSheddableBolt.SheddindMeasurableOutputCollector sheddindMeasurableCollector;
    private long lastMetricsSent;




    public DefaultSheddableBolt() {
    }

    public DefaultSheddableBolt(IRichBolt bolt){
        //_shedder = shedder;, IShedding shedder
        //_bolt = bolt;
        super(bolt);
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        int interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        executeMetric = context.registerMetric(MetricNames.TASK_EXECUTE, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        lastMetricsSent = System.currentTimeMillis();
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        sheddindMeasurableCollector = new DefaultSheddableBolt.SheddindMeasurableOutputCollector(outputCollector);
        super.prepare(conf, context, sheddindMeasurableCollector);
        //_bolt.prepare(map,topologyContext,outputCollector);
        //_collector = new OutputCollector(outputCollector);
        pendingTupleQueue = new ArrayBlockingQueue<Tuple>(tupleQueueCapacity);
        loadsheddingThread();
        LOG.info("Preparing DefaultSheddableBolt: " + context.getThisComponentId());
    }

    private long getMetricsDuration() {
        long now = System.currentTimeMillis();
        long duration = now - lastMetricsSent;
        lastMetricsSent = now;
        return duration;
    }

    private void loadsheddingThread() {

        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                ArrayList<Tuple> drainer = new ArrayList<Tuple>();
                boolean done = false;
                double shedRate = 0;
                Tuple tuple = null;
                Integer[] decision = new Integer[2];
                while (!done){
                    try {
                        tuple = pendingTupleQueue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    drainer.clear();
                    drainer.add(tuple);
                    pendingTupleQueue.drainTo(drainer);
                    new TestPrint("pending_queue_size=", drainer.size());
                    shedRate = (drainer.size() * 1.0) / tupleQueueCapacity;
                    decision[0] = tupleQueueCapacity;
                    decision[1] = drainer.size();
                    if (trigger(decision)) {
                        ArrayList<Tuple> result = (ArrayList<Tuple>) drop(shedRate, drainer);
                        for (Tuple t : result) {
                            handle(t);
                        }
                        System.out.println("ifdone!!!!!!!");
                    } else {
                        for (Tuple t : drainer){
                            handle(t);
                        }
                        System.out.println("elsedone!!!!!!!");
                    }
                }
            }

        });
        thread.start();
        LOG.info("loadshedding thread start!");
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
        System.out.println("tuple="+tuple.getValue(0));
        try {
            pendingTupleQueue.put(tuple);
            //handle();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public List drop(double shedRate, List queue) {
        System.out.println("shedrate="+ shedRate);
        Iterator<Tuple> it = queue.iterator();
        Random random = new Random();
        while(it.hasNext()){
            Tuple t = it.next();

            if(((random.nextInt(10)+1.0) / 10.0) <= shedRate){
                sheddindMeasurableCollector.fail(t);
                System.out.println("fail the tuple:"+t.toString());
                it.remove();
            }
        }
        return queue;
    }

    @Override
    public boolean trigger(Object[] arg) {
        if(Integer.valueOf(arg[1].toString()) >= (Integer.valueOf(arg[0].toString())/2)){
            return true;
        }
        return false;
    }

}

