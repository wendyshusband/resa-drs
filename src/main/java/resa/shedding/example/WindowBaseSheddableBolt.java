package resa.shedding.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.shedding.basicServices.api.IShedding;
import resa.topology.DelegatedBolt;
import resa.util.TestPrint;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kailin on 15/3/17.
 */
public class WindowBaseSheddableBolt extends DelegatedBolt implements IShedding {

    public static Logger LOG = LoggerFactory.getLogger(WindowBaseSheddableBolt.class);

    private BaseWindowedBolt _bolt;
    //private IShedding _shedder;
    private transient OutputCollector _collector;
    final int tupleQueueCapacity = 10;
    private transient BlockingQueue<Tuple> pendingTupleQueue;
    public WindowBaseSheddableBolt(BaseWindowedBolt bolt){
        _bolt = bolt;
        //_shedder = windowBaseShedder;,IShedding windowBaseShedder
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _bolt.prepare(map,topologyContext,outputCollector);
        _collector = new OutputCollector(outputCollector);
        pendingTupleQueue = new ArrayBlockingQueue<Tuple>(tupleQueueCapacity);
        windowBaseLoadsheddingThread();
    }

    private void windowBaseLoadsheddingThread() {

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
                        //drop(shedRate, drainer);

                        System.out.println("ifdone!!!!!!!");
                    } else {

                        System.out.println("elsedone!!!!!!!");
                    }
                }
            }
        });
        thread.start();
        LOG.info("window base loadshedding start!");
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("tuple="+tuple.getValue(0));
        try {
            pendingTupleQueue.put(tuple);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public int passiveDrop(Object[] arg) {
        return 0;
    }

    @Override
    public boolean trigger(Object[] arg) {
        return false;
    }
}
