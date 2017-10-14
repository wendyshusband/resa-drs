package resa.shedding.basicServices.api;

import org.apache.storm.generated.StormTopology;
import resa.shedding.basicServices.ShedRateAndAllocResult;

import java.util.Map;

/**
 * Created by kailin on 24/5/17.
 */
public interface ISheddingDecisionMaker {
    /**
     * Called when a new instance was created.
     *
     * @param conf
     * @param rawTopology
     */
    default void init(Map<String, Object> conf, StormTopology rawTopology) {
    }

    Map<String, Integer> make(ShedRateAndAllocResult newResult, Map<String, Integer> currAlloc);
}
