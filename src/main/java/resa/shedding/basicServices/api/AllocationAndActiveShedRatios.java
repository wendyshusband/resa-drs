package resa.shedding.basicServices.api;

import java.util.Map;

/**
 * Class for active shedding including suggestAllocation and activeShedRate.
 * Created by kailin on 2017/8/7.
 */
public class AllocationAndActiveShedRatios {

    private Map<String, Integer> fixedAllocation;
    private Map<String, Double> activeShedRatios;

    public AllocationAndActiveShedRatios(Map<String, Integer> fixedAllocation,
                                        Map<String, Double> activeShedRates){
        this.activeShedRatios = activeShedRates;
        this.fixedAllocation = fixedAllocation;
    }

    public void setActiveShedRates(Map<String, Double> activeShedRates) {
        this.activeShedRatios = activeShedRates;
    }

    public Map<String, Integer> getFixedAllocation() {
        return fixedAllocation;
    }

    public void setFixedAllocation(Map<String, Integer> fixedAllocation) {
        this.fixedAllocation = fixedAllocation;
    }

    public Map<String, Double> getActiveShedRates() {
        return activeShedRatios;
    }

    @Override
    public String toString() {
        return "allocation: "+(fixedAllocation).toString()+" shedding ratio:"+(activeShedRatios).toString();
    }
}
