package resa.shedding.drswithshedding;


/**
 * Created by kailin on 27/3/17.
 */
public abstract class ILearningSelectivity<T> {
    /**
     * Fit the input and output selectivity function.
     * */
    abstract double[] Fit(T... data);
}
