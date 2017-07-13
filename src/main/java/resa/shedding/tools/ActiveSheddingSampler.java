package resa.shedding.tools;

import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * Created by 44931 on 2017/7/6.
 */
public class ActiveSheddingSampler {
    private double sampleValue;
    private long counter = 0;
    private DecimalFormat df = new DecimalFormat(".#");

    public ActiveSheddingSampler(double rate) {
        df.setRoundingMode(RoundingMode.FLOOR);
        if (Double.compare(0, rate) > 0 || Double.compare(rate, 1) > 0) {
            throw new IllegalArgumentException("Bad sample rate: " + rate);
        }
        sampleValue = (10 * Double.valueOf(df.format(rate)));
        System.out.println(sampleValue+"~wangzifu");
    }

    public boolean shoudSample() {
        if (counter > sampleValue - 10){
            counter--;
            if (counter > 0) {
                return true;
            } else {
                return false;
            }
        } else {
            counter = (int) sampleValue ;
            return true;
        }
    }

    public void show() {
        System.out.println("jiebizhiai counter = "+counter+" : sampleValue = "+sampleValue);
    }
}
