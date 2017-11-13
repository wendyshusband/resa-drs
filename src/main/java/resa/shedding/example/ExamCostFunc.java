package resa.shedding.example;

import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.basicServices.api.AllocationAndActiveShedRatios;
import resa.shedding.basicServices.api.ICostFunction;

/**
 * Created by 44931 on 2017/8/7.
 */
public class ExamCostFunc implements ICostFunction {

    @Override
    public AbstractTotalCost calcCost(AllocationAndActiveShedRatios args) {
        double alloCost = args.getFixedAllocation().values().stream().mapToDouble(Number::doubleValue).sum();
        double shedCost = args.getActiveShedRates().values().stream().mapToDouble(Number::doubleValue).sum();
        System.out.println(alloCost+" examplecostfunction "+shedCost);
        return new ExamCost(alloCost, shedCost);
    }


}
