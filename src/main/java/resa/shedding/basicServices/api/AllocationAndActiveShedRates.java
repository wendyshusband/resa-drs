package resa.shedding.basicServices.api;

import java.util.Map;

/**
 * Class for active shedding including suggestAllocation and activeShedRate.
 * Created by kailin on 2017/8/7.
 */
public class AllocationAndActiveShedRates {

    private Map<String, Integer> fixedAllocation;
    private Map<String, Double> activeShedRates;

    public AllocationAndActiveShedRates(Map<String, Integer> fixedAllocation,
                                        Map<String, Double> activeShedRates){
        this.activeShedRates = activeShedRates;
        this.fixedAllocation = fixedAllocation;
    }

    public void setActiveShedRates(Map<String, Double> activeShedRates) {
        this.activeShedRates = activeShedRates;
    }

    public Map<String, Integer> getFixedAllocation() {
        return fixedAllocation;
    }

    public void setFixedAllocation(Map<String, Integer> fixedAllocation) {
        this.fixedAllocation = fixedAllocation;
    }

    public Map<String, Double> getActiveShedRates() {
        return activeShedRates;
    }

    @Override
    public String toString() {
        return "allocation: "+(fixedAllocation).toString()+" shedding rate:"+(activeShedRates).toString();
    }
}
