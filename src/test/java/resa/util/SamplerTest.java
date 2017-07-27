package resa.util;

import org.junit.Test;

/**
 * Created by kailin on 2017/7/6.
 */
public class SamplerTest {
    @Test
    public void shoudSample() throws Exception {
        Sampler sampler = new Sampler(1);
        for(int i=100;i>0; i--){
            System.out.println(sampler.shoudSample());
        }
    }

    @Test
    public void Sampler(){

    }
}