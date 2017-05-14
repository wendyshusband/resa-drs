package resa.shedding;


import org.apache.storm.task.OutputCollector;

import java.util.List;

/**
 * Created by kailin on 20/4/17.
 */
public abstract class AbstractRandomShedding  implements IShedding{

    public abstract void randomDrop(List queue, double shedRate, OutputCollector outputCollector);

    public abstract boolean randomTrigger(int tupleQueueCapacity,int allTupleSize);

    @Override
    public void drop(Object[] arg) {

    }

    @Override
    public boolean trigger(Object[] arg) {
        return false;
    }
}
