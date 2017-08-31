package resa.shedding.basicServices.api;


import org.apache.storm.task.OutputCollector;

import java.util.List;

/**
 * Created by kailin on 20/4/17.
 */
public abstract class AbstractRandomShedding implements IShedding {

    public abstract void randomDrop(List queue, double shedRate, OutputCollector outputCollector);

    public abstract boolean randomTrigger(int tupleQueueCapacity,int allTupleSize);

    @Override
    public int passiveDrop(Object[] arg) {
        return 0;
    }

    @Override
    public boolean trigger(Object[] arg) {
        return false;
    }
    
}
