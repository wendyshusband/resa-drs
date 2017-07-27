package resa.shedding.tools;

import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * Created by kailin on 2017/7/6.
 */
public class ActiveSheddingSampler {
    private double sampleValue;
    private long counter = 0;
    private DecimalFormat df = new DecimalFormat(".##");
    private DecimalFormat df2 = new DecimalFormat(".#");
    public ActiveSheddingSampler(double rate) {
        df.setRoundingMode(RoundingMode.FLOOR);
        if (Double.compare(0, rate) > 0 || Double.compare(rate, 1) > 0) {
            throw new IllegalArgumentException("Bad sample rate: " + rate);
        }
        //sampleValue = (10.0 * Double.valueOf(df2.format(rate)));
        sampleValue = 10 - (Double.valueOf(String.format("%.2f",rate)) * 10);
        System.out.println(sampleValue+"~wangzifu");
    }

    public boolean shoudSampleLowPrecision() {
        if (counter > sampleValue - 10){
            counter--;
            if (counter + 1 > 0) {
                return true;
            } else {
                return false;
            }
        } else {
            counter = (int) sampleValue - 1;
            return true;
        }
    }

    public boolean shoudSample() {

        if (counter > sampleValue * 10 - 100){
            counter--;
            if (counter + 1 > 0) {
                return false;
            } else {
                return true;
            }
        } else {
            counter = (int) (sampleValue * 10 - 1);
            return false;
        }
    }

    public void show() {
        System.out.println("jiebizhiai counter = "+counter+" : sampleValue = "+sampleValue);
    }
}
