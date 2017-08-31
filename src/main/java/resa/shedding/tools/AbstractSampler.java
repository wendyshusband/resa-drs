package resa.shedding.tools;

/**
 * Created by 44931 on 2017/8/6.
 */
public abstract class AbstractSampler {

    protected double sampleValue;
    public abstract boolean shoudSample();

    public double getSampleValue() {
        return sampleValue;
    }

}
