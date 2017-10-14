package resa.shedding.example;

import resa.shedding.basicServices.api.AbstractTotalCost;

/**
 * Created by 44931 on 2017/9/4.
 */
public class OutdetCost extends AbstractTotalCost {

    public OutdetCost(double resourceCost, double shedCost) {
        super(resourceCost, shedCost);
    }

    @Override
    public double calcTotalCost(double resourceCost, double shedCost) {
        double cost = resourceCost+shedCost;
        return cost;
    }
}
