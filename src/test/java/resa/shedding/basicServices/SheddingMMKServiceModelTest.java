package resa.shedding.basicServices;

import org.junit.Test;

/**
 * Created by kailin on 2017/7/4.
 */
public class SheddingMMKServiceModelTest {
    @Test
    public void findAllocationGeneralTopApplyMMK() throws Exception {
    }

    @Test
    public void suggestAllocationWithShedRate() throws Exception {

    }

    @Test
    public void avgSojurnTime() throws Exception {
        System.out.println(SheddingMMKServiceModel.avgQueueingTime_MMK(41.364236342314,41.364236342313,1));
    }
}