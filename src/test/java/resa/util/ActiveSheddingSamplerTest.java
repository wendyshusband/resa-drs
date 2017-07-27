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
}