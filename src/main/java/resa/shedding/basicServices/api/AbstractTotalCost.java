package resa.shedding.basicServices.api;

import org.jetbrains.annotations.NotNull;

/**
 * Created by kailin on 2017/8/7.
 */
public abstract class AbstractTotalCost implements Comparable {

    private double resourceCost;
    private double shedCost;
    private double totalCost;

    public void setCost(double resourceCost, double shedCost) {
        this.resourceCost = resourceCost;
        this.shedCost = shedCost;
        totalCost = calcTotalCost(resourceCost, shedCost);
    }

    public AbstractTotalCost(double resourceCost, double shedCost) {
        this.resourceCost = resourceCost;
        this.shedCost = shedCost;
        totalCost = calcTotalCost(resourceCost, shedCost);
    }

    public abstract double calcTotalCost(double resourceCost, double shedCost);

    @Override
    public int compareTo(@NotNull Object o) {
        if (totalCost > ((AbstractTotalCost) o).getTotalCost()) {
            return 1;
        } else if (totalCost == ((AbstractTotalCost) o).getTotalCost()) {
            return 0;
        } else {
            return -1;
        }
    }

    public double getTotalCost() {
        return totalCost;
    }

    @Override
    public String toString() {
        return "{resource cost: "+resourceCost+" shed cost: "+shedCost+" total cost: "+totalCost+"}";
    }
}
