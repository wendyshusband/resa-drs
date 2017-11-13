package resa.shedding.example;

import org.jetbrains.annotations.TestOnly;
import redis.clients.jedis.Jedis;
import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.tools.TestRedis;

import java.util.Date;

/**
 * Created by 44931 on 2017/9/27.
 */
public class FPAndODCost extends AbstractTotalCost {

    private static final double threshold = 0.87;
    private static final double costThreshold = 17.0;

    private static final Jedis jedis = TestRedis.getJedis();
    private static String name = jedis.get("type");

    private static double odAccuracySensitive(double shedCost) {
        //double res = 0.93298262 * Math.exp(-1 * 4.91578576 * shedCost) + 0.06391202;
        double res = -1.1 * shedCost + 0.87;
        System.out.println(shedCost+"accuracytianshichibang:"+res);
        if (res <= threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    private static double fpAccuracySensitive(double shedCost) {
        //double res = -0.95625272727 * shedCost + 0.851328;
        double res = -1 * shedCost + 0.81;
        System.out.println("accuracytianshichibang:"+res);
        if (res < threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    private static double costSensitive(double resourceCost, double shedCost) {
        double cost = resourceCost + 100 * shedCost;
        if (resourceCost > costThreshold) {
            return Double.MAX_VALUE;
        } else {
            return cost;
        }
    }

    public FPAndODCost(double resourceCost, double shedCost) {
        super(resourceCost, shedCost);
    }

    @Override
    public double calcTotalCost(double resourceCost, double shedCost) {
        double cost;
        long start = Long.valueOf(jedis.get("time"));
        Date date = new Date();
        if (start > 54999 && start < 100000) {
            System.out.println(name+" wtfcostis:accuracysensitive"+start+"now"+date.toString());
            if (name.equals("od")) {
                cost = resourceCost + odAccuracySensitive(shedCost);
            } else {
                cost = resourceCost + fpAccuracySensitive(shedCost);
            }
        } else {
            System.out.println(name+" wtfcostis:costsensitive"+start+"now"+System.currentTimeMillis());
            cost = costSensitive(resourceCost,shedCost);
        }
        return cost;
    }
}
