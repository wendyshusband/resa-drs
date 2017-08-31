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
import resa.shedding.basicServices.api.AbstractRandomShedding;
import resa.topology.DelegatedBolt;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kailin on 19/4/17.
 */
public class RandomSheddableBolt extends DelegatedBolt {
    public static Logger LOG = LoggerFactory.getLogger(RandomSheddableBolt.class);
    protected class SheddindMeasurableOutputCollector extends OutputCollector {

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

        @Override
        public void fail(Tuple input) {
            super.fail(input);
        }
    }

    private int tupleQueueCapacity;
    private transient BlockingQueue<Tuple> pendingTupleQueue;
    private transient MultiCountMetric passiveSheddingRateMetric;
    private AbstractRandomShedding _shedder;
    private transient CMVMetric executeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private transient RandomSheddableBolt.SheddindMeasurableOutputCollector sheddindMeasurableCollector;
    private long lastMetricsSent;

    public RandomSheddableBolt(IRichBolt bolt, AbstractRandomShedding shedder){
        super(bolt);
        this._shedder = shedder;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        int interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        executeMetric = context.registerMetric(MetricNames.TASK_EXECUTE, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        passiveSheddingRateMetric = context.registerMetric(MetricNames.SHEDDING_RATE, new MultiCountMetric(),interval);
        lastMetricsSent = System.currentTimeMillis();
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        tupleQueueCapacity = ConfigUtil.getInt(conf,ResaConfig.TUPLE_QUEUE_CAPACITY,1024);
        sheddindMeasurableCollector = new RandomSheddableBolt.SheddindMeasurableOutputCollector(outputCollector);
        super.prepare(conf, context, sheddindMeasurableCollector);
        pendingTupleQueue = new ArrayBlockingQueue<>(tupleQueueCapacity);
        loadsheddingThread();
        LOG.info("Preparing SheddableBolt: " + context.getThisComponentId());
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
                double shedRate;
                Tuple tuple = null;
                while (!done){
                    try {
                        tuple = pendingTupleQueue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    drainer.clear();
                    drainer.add(tuple);
                    pendingTupleQueue.drainTo(drainer);
                    shedRate = (drainer.size() * 1.0) / tupleQueueCapacity;
                    int originSize = drainer.size();
                    System.out.println("originSize: "+originSize);
                    passiveSheddingRateMetric.scope("allTuple").incrBy(drainer.size());
                    if (_shedder.randomTrigger(tupleQueueCapacity,drainer.size())) {
                        _shedder.randomDrop(drainer,shedRate,sheddindMeasurableCollector);
                        int increment = originSize - drainer.size();
                        passiveSheddingRateMetric.scope("dropTuple").incrBy(increment);
                    } else {
                        passiveSheddingRateMetric.scope("dropTuple").incrBy(0);
                    }
                    for (Tuple t : drainer) {
                        handle(t);
                    }
                }
            }
        });
        thread.start();
        LOG.info("loadshedding thread start!");
    }

    public void execute(Tuple in) {
        try {
            pendingTupleQueue.put(in);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
}
