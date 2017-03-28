package resa.shedding.drswithshedding;

/**
 * Created by kailin on 27/3/17.
 */
public class OperatorData {

    public final long timestamp;
    public final String component;
    public final int task;
    public final double sheddingRate;

    public OperatorData(long timestamp, String component, int task ,double sheddingRate) {
        this.sheddingRate = sheddingRate;
        this.timestamp = timestamp;
        this.component = component;
        this.task = task;
    }
}
