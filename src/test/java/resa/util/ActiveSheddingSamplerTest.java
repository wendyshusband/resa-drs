package resa.util;

import org.junit.Test;
import resa.shedding.tools.ActiveSheddingSampler;

/**
 * Created by kailin on 2017/7/6.
 */
public class ActiveSheddingSamplerTest {

    @Test
    public void shoudSample() throws Exception {
        double rate = 0.035;
        //ActiveSheddingSampler sampler = new ActiveSheddingSampler(rate);
        int j = 0;
        while (j < 9) {
            rate = rate+0.1;
            System.out.println(rate);
            ActiveSheddingSampler sampler = new ActiveSheddingSampler(rate);
            int count = 0;
            int i=1;
            while (i <= 10) {
                //System.out.println(rate+"~");
                //sampler.show();
                boolean a = sampler.shoudSampleLowPrecision();
                //System.out.println(a);
                if (rate != 0 && a) {
                    if (a) {
                        count++;
                    }
                }
                i++;
            }
            System.out.println(j+"count="+count);
            j++;
        }

    }

    @Test
    public void highPrecision() throws Exception {
        double rate = 0.0334534;
        //ActiveSheddingSampler sampler = new ActiveSheddingSampler(rate);
        int i = 1;
//        while (i<=10) {
//            //ActiveSheddingSampler sampler = new ActiveSheddingSampler(i/10.0);
//            System.out.println(sampler.shoudSample());
//            i++;
//        }
        int j = 0;
        while (j < 1) {
            rate = rate+0.2;
            //System.out.println(rate);
            ActiveSheddingSampler sampler = new ActiveSheddingSampler(rate);
            int count = 0;
            i=1;
            while (i <= 100) {
                //System.out.println(rate+"~");
                boolean a = sampler.shoudSample();
                System.out.println(a);
                if (rate != 0 && a) {
                    if (a) {
                        count++;
                    }
                }
                i++;
            }
            System.out.println(j+"count="+count);
            j++;
        }
    }

    @Test
    public void newFunction() throws Exception {
        long start = System.currentTimeMillis();
        double rate = -0.05;
        //ActiveSheddingSampler sampler = new ActiveSheddingSampler(rate);
        int i = 1;
//        while (i<=10) {
//            //ActiveSheddingSampler sampler = new ActiveSheddingSampler(i/10.0);
//            System.out.println(sampler.shoudSample());
//            i++;
//        }
        int j = 0;
        while (j < 9) {
            rate = rate + 0.1;
            //System.out.println(rate);
            ActiveSheddingSampler sampler = new ActiveSheddingSampler(rate);
            int count = 0;
            i = 1;
            while (i <= 10000000) {
//                if (i % 100 == 0) {
//                    System.out.println("i= "+i);
//                }
                //System.out.println(rate+"~");
                boolean a = sampler.shoudSample();
                //System.out.println(a);
                if (rate != 0 && a) {
                    if (a) {
                        count++;
                    }
                }
                i++;
            }
            System.out.println(j + "count=" + count);
            j++;
        }
        System.out.println(System.currentTimeMillis()-start);
    }

    @Test
    public void checkCase() {
        int sampledRemainder = 1;
        int notBeSampledRemainder = 1;
        System.out.println((sampledRemainder != 0 && notBeSampledRemainder != 0) ? 1 :
                ((sampledRemainder == 0 && notBeSampledRemainder != 0) ? 2 : (
                        (sampledRemainder != 0 && notBeSampledRemainder == 0) ? 3 :4)));

        System.out.println((int) (((10 - 5.6000000000000005) * 10) % 6));
        System.out.println(0.6+-0.04);
    }
}