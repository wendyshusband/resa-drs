package resa.shedding.drswithshedding;


/**
 * Created by kailin on 27/3/17.
 */
public abstract class LearningSelectivity<T> {
    /**
     * Fit the input and output selectivity function.
     * */
    public abstract double[] Fit(T... data);


}
