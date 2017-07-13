package resa.util;

import org.junit.Test;
import resa.shedding.tools.ActiveSheddingSampler;

/**
 * Created by 44931 on 2017/7/6.
 */
public class ActiveSheddingSamplerTest {

    @Test
    public void shoudSample() throws Exception {
        ActiveSheddingSampler sampler = new ActiveSheddingSampler(0.63251513213);
        int i = 1;
        while (i<=10) {
            //ActiveSheddingSampler sampler = new ActiveSheddingSampler(i/10.0);
            System.out.println(sampler.shoudSample());
            i++;
        }

    }
}