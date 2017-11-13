package resa.shedding.example;

import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.basicServices.api.AllocationAndActiveShedRatios;
import resa.shedding.basicServices.api.ICostFunction;

/**
 * Created by 44931 on 2017/9/4.
 */
public class OutdetCostFunc implements ICostFunction {

    private static final double threshold =  0.5;

    private static double powerFunc(double shedCost) {
        double res = 0.93298262 * Math.exp(-1 * 4.91578576 * shedCost) + 0.06391202;
        System.out.println("accuracytianshichibang:"+res);
        if (res < threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    private static double linerFunc(double shedCost) {
        double res = 0.93298262 * Math.exp(-1 * 4.91578576 * shedCost) + 0.06391202;
        System.out.println("accuracytianshichibang:"+res);
        if (res < threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    @Override
    public AbstractTotalCost calcCost(AllocationAndActiveShedRatios args) {
        double alloCost = args.getFixedAllocation().values().stream().mapToDouble(Number::doubleValue).sum();
        double shedCost = args.getActiveShedRates().values().stream().mapToDouble(Number::doubleValue).sum();
        //return new OutdetCost(alloCost, powerFunc(shedCost));
        return new OutdetCost(alloCost, powerFunc(shedCost));
    }
}
