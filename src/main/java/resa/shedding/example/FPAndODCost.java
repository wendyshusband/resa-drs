package resa.shedding.example;

import redis.clients.jedis.Jedis;
import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.tools.TestRedis;

import java.util.Date;

/**
 * Created by 44931 on 2017/9/27.
 */
public class FPAndODCost extends AbstractTotalCost {

    private static final double threshold =  0.5;
    private static final double costThreshold = 25.0;

    private static final Jedis jedis = TestRedis.getJedis();
    private static String name = jedis.get("type");
    private static double odAccuracySensitive(double shedCost) {
        double res = 0.93298262 * Math.exp(-1 * 4.91578576 * shedCost) + 0.06391202;
        System.out.println("accuracytianshichibang:"+res);
        if (res < threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    private static double fpAccuracySensitive(double shedCost) {
        double res = -0.95625272727 * shedCost + 0.851328;
        System.out.println("accuracytianshichibang:"+res);
        if (res < threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    private static double costSensitive(double resourceCost, double shedCost) {
        double cost = resourceCost + 100 * shedCost;
        if (resourceCost >= costThreshold) {
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
        double cost = 0.0;
        long start = Long.valueOf(jedis.get("time"));
        if (start == 22501) {
            Date date = new Date();
            System.out.println(date+" wtfcostis:yaoqiehuanlo"+start);
        }
        if (start > 22500) {
            System.out.println(name+" wtfcostis:accuracysensitive"+start+"now"+System.currentTimeMillis());
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
