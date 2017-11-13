package resa.shedding.basicServices.api;

/**
 * Created by 44931 on 2017/8/7.
 */
public interface ICostFunction {

    AbstractTotalCost calcCost(AllocationAndActiveShedRatios args);
}
