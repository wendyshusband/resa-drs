package resa.shedding.basicServices.api;


/**
 * Created by kailin on 6/3/17.
 */
public interface IShedding<T> {
    /**
     * passive drop operate
     * @param arg every number of argument
     * @return drop number
     */
    int passiveDrop(T... arg);

    /**
     * passive shedding trigger
     * @param arg every number of argument
     */
    boolean trigger(T... arg);
}
