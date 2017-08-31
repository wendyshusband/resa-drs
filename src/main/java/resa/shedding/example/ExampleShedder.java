package resa.shedding.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import resa.shedding.basicServices.api.AbstractRandomShedding;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kailin on 19/4/17.
 */
public class ExampleShedder extends AbstractRandomShedding implements Serializable {


    @Override
    public void randomDrop(List queue, double shedRate, OutputCollector outputCollector) {
        Iterator<Tuple> it = queue.iterator();
        int count = 0;
        int shedTupleCount = (int) (queue.size() * shedRate);
        while (it.hasNext() && count <= shedTupleCount) {
            Tuple t = it.next();
            outputCollector.fail(t);
            it.remove();
            count++;
        }
    }

    @Override
    public boolean randomTrigger(int tupleQueueCapacity, int allTupleSize) {
        if (allTupleSize >= tupleQueueCapacity / 2) {
            return true;
        }
        return false;
    }

}