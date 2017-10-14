package resa.shedding.tools;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Random;

/**
 * Created by kailin on 2017/7/6.
 */
public class ActiveSheddingSampler extends AbstractSampler {

    private long counter = 0;
    private DecimalFormat df = new DecimalFormat(".##");
    private DecimalFormat df2 = new DecimalFormat(".#");
    private int sampledQuotient;
    private int notBeSampledQuotient;
    private int sampledRemainder;
    private int notBeSampledRemainder;
    private int divisor;
    private int checker;
    private int divisorCount = 0;
    private static Random random = new Random();

    public ActiveSheddingSampler(double rate) {
        df.setRoundingMode(RoundingMode.FLOOR);
        if (Double.compare(0, rate) > 0 || Double.compare(rate, 1) > 0) {
            throw new IllegalArgumentException("Bad sample rate: " + rate);
        }
        //sampleValue = (10.0 * Double.valueOf(df2.format(rate)));
        //sampleValue = 10 - (Double.valueOf(String.format("%.2f",rate)) * 10);
        sampleValue = Double.valueOf(String.format("%.2f",rate)) * 10;

        divisor = (int) Math.ceil(sampleValue);
        sampledQuotient = (int) ((Double.valueOf(String.format("%.2f",10 *sampleValue)))  / divisor);
        sampledRemainder = (int) ((Double.valueOf(String.format("%.2f",10 * sampleValue)))  % divisor);
        notBeSampledQuotient = (int) (((Double.valueOf(String.format("%.2f",10 - sampleValue))) * 10) / divisor);
        notBeSampledRemainder = (int) (((Double.valueOf(String.format("%.2f",10 - sampleValue))) * 10) % divisor);
        checker = checkCase();
        System.out.println(sampleValue+"~wangzifu");
        System.out.println(divisor+"~"+sampledQuotient+"~"+sampledRemainder+"~"+notBeSampledQuotient+"~"+notBeSampledRemainder);
    }

    private int checkCase() {
        return  (sampledRemainder != 0 && notBeSampledRemainder != 0) ? 0 :
                ((sampledRemainder == 0 && notBeSampledRemainder != 0) ? 1 : (
                        (sampledRemainder != 0 && notBeSampledRemainder == 0) ? 2 :3));
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

    public boolean shoudSampleBatch() {

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

    private boolean newShoudSampleHelper() {
        if (sampledQuotient == 0) {
            counter = 1;
            return false;
        } else {
            counter = 1;
            return true;
        }
    }

    private boolean newShoudSampleHelper2() {
        if (sampledRemainder > 0) {
            counter = 1;
            divisorCount = 0;
            return true;
        } else if (notBeSampledRemainder > 0) {
            counter = 1;
            divisorCount = 0;
            return false;
        } else {
            divisorCount = 1;
            return newShoudSampleHelper();
        }
    }

    private boolean shoudSampleSplitBatch() {
        if (divisorCount <= 0) {
            if (sampledRemainder > 0 && counter < sampledRemainder) {
                counter++;
                return true;
            } else if (notBeSampledRemainder > 0 && counter < (sampledRemainder + notBeSampledRemainder)) {
                counter++;
                return false;
            } else {
                divisorCount++;
                return newShoudSampleHelper();
            }
        } else {
            if (divisorCount <= divisor) {
                if (counter < sampledQuotient) {
                    counter++;
                    return true;
                } else if (notBeSampledQuotient != 0 && counter < (sampledQuotient + notBeSampledQuotient)) {
                    counter++;
                    return false;
                } else {
                    divisorCount++;
                    if (divisorCount > divisor) {
                        return newShoudSampleHelper2();
                    } else {
                        return newShoudSampleHelper();
                    }

                }
            } else {
                return newShoudSampleHelper2();
            }
        }
    }

    private boolean randomSampler() {
        return random.nextInt(100) < sampleValue;
    }

    @Override
    public boolean shoudSample() {
        return shoudSampleSplitBatch();
        //return randomSampler();
    }

    public double getSampleValue() {
        return sampleValue;
    }
}
