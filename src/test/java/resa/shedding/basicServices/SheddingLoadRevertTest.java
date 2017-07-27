package resa.shedding.basicServices;

import org.junit.Test;

/**
 * Created by kailin on 2017/7/17.
 */
public class SheddingLoadRevertTest {

    @Test
    public void revertCompleteLatencyTest() throws Exception {
        long tempFailCount = 0;
        long tempDropCount = 2214;
        long tempAllCount = 8869;//serviceNodeMap.values().stream().mapToLong(ServiceNode::getAllCount).sum();
        double completeLatency = 252.235;
        double realCL = (((tempAllCount-tempFailCount)*completeLatency)
                +((tempDropCount+tempFailCount) * 30 * 1000))/(tempAllCount+tempDropCount);
        //sourceNode.revertCompleteLatency(realCL);
        System.out.println("all:"+tempAllCount+"fail:"+tempFailCount+"drop:"+tempDropCount+"processdrop"+realCL/0.482);
        System.out.println((tempAllCount-tempFailCount)*completeLatency);
        System.out.println((tempDropCount+tempFailCount) * 30 * 1000);
    }
}