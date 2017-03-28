package resa.shedding;


import org.apache.storm.task.OutputCollector;

import java.util.List;

/**
 * Created by kailin on 6/3/17.
 */
public interface IShedding<E,T> {
    /**
     * drop operate
     * @param shedRate
     * @param queue a list of tuple
     */
    List<E> drop(double shedRate, List<E> queue);

    /**
     * shedding trigger operate
     * @param arg
     */
    boolean trigger(T... arg);
}
