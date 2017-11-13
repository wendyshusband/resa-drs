package resa.shedding.basicServices;

import resa.optimize.AllocResult;

import java.util.Map;

/**
 * Created by kailin on 24/5/17.
 */
public class ShedRateAndAllocResult {

    private AllocResult allocResult;
    private Map<String, Map<String,Double>> activeShedRatio;
    public ShedRateAndAllocResult(AllocResult.Status status, Map<String, Integer> minReqOptAllocation,
                                  Map<String, Integer> currOptAllocation, Map<String, Integer> kMaxOptAllocation,
                                  Map<String, Map<String,Double>> activeShedRate, Map<String, Object> ctx){
        this.allocResult = new AllocResult(status, minReqOptAllocation, currOptAllocation, kMaxOptAllocation).setContext(ctx);
        this.activeShedRatio = activeShedRate;
    }

    public AllocResult getAllocResult() {
        return this.allocResult;
    }

    public Map<String, Map<String,Double>> getActiveShedRatio() {
        return activeShedRatio;
    }

    @Override
    public String toString() {
        return "{Active shedding ratio: "+activeShedRatio.toString()+" Allocation: "+allocResult+"}";
    }
}
