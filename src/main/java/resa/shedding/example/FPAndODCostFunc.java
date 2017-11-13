package resa.shedding.example;

import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.basicServices.api.AllocationAndActiveShedRatios;
import resa.shedding.basicServices.api.ICostFunction;

/**
 * Created by 44931 on 2017/9/27.
 */
public class FPAndODCostFunc implements ICostFunction {

    @Override
    public AbstractTotalCost calcCost(AllocationAndActiveShedRatios args) {
        double alloCost = args.getFixedAllocation().values().stream().mapToDouble(Number::doubleValue).sum();
        double shedCost = args.getActiveShedRates().values().stream().mapToDouble(Number::doubleValue).sum();
        return new FPAndODCost(alloCost, shedCost);
    }
}
