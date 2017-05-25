package resa.shedding.basicServices;

import resa.optimize.AllocResult;

import java.util.Map;

/**
 * Created by kailin on 24/5/17.
 */
public class ShedRateAndAllocResult {

    public AllocResult allocResult;
    public Map<String,Double> activeShedRate;
    public ShedRateAndAllocResult(AllocResult.Status status, Map<String, Integer> minReqOptAllocation,
                                  Map<String, Integer> currOptAllocation, Map<String, Integer> kMaxOptAllocation,
                                  Map<String, Double> activeShedRate, Map<String, Object> ctx){
        allocResult = new AllocResult(status, minReqOptAllocation, currOptAllocation, kMaxOptAllocation);
        allocResult.setContext(ctx);
        this.activeShedRate = activeShedRate;
    }

    public AllocResult getAllocResult() {
        return allocResult;
    }

    public Map<String, Double> getActiveShedRate() {
        return activeShedRate;
    }

}
