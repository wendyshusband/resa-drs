package resa.shedding;


import org.apache.storm.task.OutputCollector;

import java.util.List;

/**
 * Created by kailin on 6/3/17.
 */
public interface IShedding<T> {
    /**
     * drop operate
     * @param arg every number of argument
     */
    void drop(T... arg);

    /**
     * shedding trigger
     * @param arg every number of argument
     */
    boolean trigger(T... arg);
}
