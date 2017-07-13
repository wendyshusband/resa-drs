package resa.shedding.basicServices;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.optimize.AllocResult;
import resa.optimize.ServiceNode;
import resa.optimize.SourceNode;
import resa.shedding.example.PolynomialRegression;
import resa.shedding.tools.HistoricalAdjustRatioMMK;

import java.util.HashMap;
import java.util.Map;
import java.util.function.DoublePredicate;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on May-8-2017
 */
public class SheddingMMKServiceModel implements SheddingServiceModel {

    private static final Logger LOG = LoggerFactory.getLogger(SheddingMMKServiceModel.class);
    private HistoricalAdjustRatioMMK paramPairForCalcAdjRatio = new HistoricalAdjustRatioMMK(5);

    /**
     * Class for active shedding including suggestAllocation and activeShedRate.
     * */
    private static class NewAllocationAndActiveShedRates {
        public Map<String, Integer> fixedAllocation;
        public Map<String, Double> activeShedRates;

        public NewAllocationAndActiveShedRates(Map<String, Integer> fixedAllocation,
                Map<String, Double> activeShedRates){
            this.activeShedRates = activeShedRates;
            this.fixedAllocation = fixedAllocation;
        }

        public void setActiveShedRates(Map<String, Double> activeShedRates) {
            this.activeShedRates = activeShedRates;
        }

        public Map<String, Integer> getFixedAllocation() {
            return fixedAllocation;
        }

        public void setFixedAllocation(Map<String, Integer> fixedAllocation) {
            this.fixedAllocation = fixedAllocation;
        }

        public Map<String, Double> getActiveShedRates() {
            return activeShedRates;
        }

        @Override
        public String toString() {
            return "allocation: "+((HashMap)fixedAllocation).toString()+" shedding rate:"+((HashMap)activeShedRates).toString();
        }
    }

    /**
     * We assume the stability check for each node is done beforehand!
     * Jackson OQN assumes all the arrival and departure are iid and exponential
     *
     * Note, the return time unit is in Second!
     *
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    static double getExpectedTotalSojournTimeForJacksonOQN(Map<String, ServiceNode> serviceNodes, Map<String, Integer> allocation) {
        double retVal = 0.0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            ServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();

            double avgSojournTime = sojournTime_MMK(serviceNode.getLambda(), serviceNode.getMu(), serverCount);
            //System.out.println(serviceNode.getComponentID()+"!!!!"+serverCount+"renmindemingyi"+avgSojournTime);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        System.out.println("retvalheheda: "+retVal);
        return retVal;
    }

    /**
     * @param sourceNode
     * @param serviceNodes
     * @param totalResourceCount
     * @param messageTimeOut
     * @param relativeE
     * @param selectivityFunctions (for bolt active shedding when choice shedding location)
     * @param targets (for bolt active shedding when choice shedding location)
     * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
     * b) total minReq can not be satisfied (total minReq > totalResourceCount)
     * otherwise, the Map data structure.
     */
    static NewAllocationAndActiveShedRates suggestAllocationWithShedRate(SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount,
                                                                         double messageTimeOut, double relativeE, Map<String, double[]> selectivityFunctions, Map<String, Object> targets, Map<String, Integer> currBoltAllocation) {
        double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
        if (serviceNodes.values().stream().mapToDouble(ServiceNode::getMu).anyMatch(new DoublePredicate() {
            @Override
            public boolean test(double value) {
                if (value == 0.0){
                    return true;
                }
                return  false;
            }
        })) {
            System.out.println("mu = 0 gaogemao!");
            serviceNodes.values().stream().forEach(e->{
                System.out.println(e.getComponentID()+"`s mu ="+e.getMu());
            });
            return null;
        }

        Map<String, Integer> tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalResourceCount);
        Map<String, Double> activeShedRateMap = new HashMap<>();
        serviceNodes.keySet().stream().forEach(e->{
            activeShedRateMap.put(e,0.0);
        });
        double estimateT = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes,currBoltAllocation);
        double adjRatio = 1.0;//Math.max(1.0, sourceNode.getRealLatencySeconds() / estimateT);
        System.out.println(estimateT+"feozjp: "+sourceNode.getRealLatencySeconds());
        if (tempAllocation == null) {// need shedding

            double highMark = sourceNode.getTupleEmitRateOnSQ();
            double lowMark = 0.0;
            double lambda0 = (lowMark + highMark) / 2;
            //Map<String, Integer> fixedAllocation = new HashMap<>();

            while (lambda0 > 1 && (highMark > lowMark)) {
                for (ServiceNode serviceNode:serviceNodes.values()) {
                    //System.out.println(serviceNode.getComponentID()+" : "+lambda0+"~~~~~~~~~"+serviceNode.getRatio());
                    serviceNode.changeLambdaAndOtherRelateParam(lambda0 * serviceNode.getRatio(),lambda0);
                }
                tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes,totalResourceCount);
                System.out.println("lambda0: "+lambda0);
                if (tempAllocation != null) {
                    System.out.println("tempallocation: "+tempAllocation);
                    estimateT = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes,tempAllocation);
                    System.out.println(adjRatio+" timeout: "+messageTimeOut+"estmateT: "+estimateT * 1000.0 * adjRatio);
                    if (((estimateT * 1000.0 * adjRatio) < messageTimeOut)
                            && (((estimateT * 1000.0 * adjRatio) / messageTimeOut) > relativeE)) {
                        sourceNode.revertLambda(lambda0);
                        sourceNode.revertCompleteLatency(estimateT * 1000.0 * adjRatio);
                        break;
                    } else {
                        if ((estimateT * 1000.0 * adjRatio) > messageTimeOut) {
                            highMark = lambda0;
                            lambda0 = (lowMark + highMark) / 2;
                        } else {
                            lowMark = lambda0;
                            lambda0 = (lowMark + highMark) / 2;
                        }
                    }
                } else {
                    highMark = lambda0;
                    lambda0 = (lowMark + highMark) / 2;
                    System.out.println(lowMark+"dangyuan "+lambda0);
                }
            }
            System.out.println("high:"+highMark+" low:"+lowMark);
            System.out.println("final lambda0: "+lambda0+" timeout: "+messageTimeOut+" rate: "+(1 - ( lambda0 / originLambda0)));
            activeShedRateMap.put(sourceNode.getComponentID(), (1 - ( lambda0 / originLambda0)));
//            for (ServiceNode serviceNode:serviceNodes.values()) {
//                serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(),lambda0);
//            }
            return new NewAllocationAndActiveShedRates(tempAllocation,activeShedRateMap);
        } else {
            System.out.println("no need shedding gaogemao!");
            activeShedRateMap.put(sourceNode.getComponentID(),0.0);
            return new NewAllocationAndActiveShedRates(tempAllocation,activeShedRateMap);
        }
    }

        /**
         * @param serviceNodes
         * @param totalResourceCount
         * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
         * b) total minReq can not be satisfied (total minReq > totalResourceCount)
         * otherwise, the Map data structure.
         */
    static Map<String, Integer> suggestAllocationGeneralTopApplyMMK(Map<String, ServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply M/M/K, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                    double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        ServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                        double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    /**
     * Like Module A', input required QoS, output #threads required
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param sourceNode
     * @param estTotalSojournTimeMilliSec_MMK
     * @param serviceNodes
     * @param completeTimeMilliSecUpper
     * @param completeTimeMilliSecLower
     * @param currentUsedThreadByBolts
     * @param maxAvailableExec
     * @return null when status is INFEASIBLE; or FEASIBLE reallocation (with resource added)
     */
    static NewAllocationAndActiveShedRates getMinReqServerAllocationAndShedRateGeneralTopApplyMMK(SourceNode sourceNode, double estTotalSojournTimeMilliSec_MMK, Map<String, ServiceNode> serviceNodes,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower, int currentUsedThreadByBolts, int maxAvailableExec, double relativeE, Map<String, Integer> currAllocation, double adjRatio) {

        double lowerBoundServiceTimeSeconds = 0.0;  //in seconds
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            ///caution, the unit should be millisecond
            lowerBoundServiceTimeSeconds += (1.0 / mu);
            totalMinReq += getMinReqServerCount(lambda, mu);
        }
        if (adjRatio == 0) {
            adjRatio = Math.max(1.0, sourceNode.getRealLatencyMilliSeconds() / estTotalSojournTimeMilliSec_MMK);
        }
       // double adjRatio = SheddingMMKServiceModel.calcAdjRatio();
                //Math.max(1.0, sourceNode.getRealLatencySeconds() / estTotalSojournTimeMilliSec_MMK);
        System.out.println("lowerBoundServiceTimeSeconds: "+lowerBoundServiceTimeSeconds * adjRatio * 1000.0);
        System.out.println(estTotalSojournTimeMilliSec_MMK+"laibuliao: "+sourceNode.getRealLatencyMilliSeconds());
        if (lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec) {
            LOG.debug(" getMinReqServerAllocationGeneralTopApplyMMK(), " +
                    "lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec");

            double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
            Map<String, Double> activeShedRateMap = new HashMap<>();
            serviceNodes.keySet().stream().forEach(e -> {
                activeShedRateMap.put(e, 0.0);
            });
            Map<String, Integer> tempAllocation = currAllocation;
            double estimateT;
            double highMark = sourceNode.getTupleEmitRateOnSQ();
            double lowMark = 0.0;
            double lambda0 = (lowMark + highMark) / 2;
            //Map<String, Integer> fixedAllocation = new HashMap<>();
            while (lambda0 >= 1 && highMark > lowMark) {
                for (ServiceNode serviceNode : serviceNodes.values()) {
                    //System.out.println(serviceNode.getComponentID() + " : " + lambda0 + "~~~~~~~~~" + serviceNode.getRatio());
                    serviceNode.changeLambdaAndOtherRelateParam(lambda0 * serviceNode.getRatio(), lambda0);
                }
                System.out.println("lambda0: " + lambda0);
                tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes,currentUsedThreadByBolts);
                estimateT = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, tempAllocation);
                //double adjRatio = Math.max(1.0, sourceNode.getRealLatencySeconds() / estimateT);
                System.out.println(adjRatio+" completeTimeMilliSecUpper: "+completeTimeMilliSecUpper+"estmateT: "+estimateT * 1000.0 * adjRatio);
                if (((estimateT * 1000.0 * adjRatio) < completeTimeMilliSecUpper) && (((estimateT * 1000.0 * adjRatio) / completeTimeMilliSecUpper) > relativeE)) {
                    sourceNode.revertLambda(lambda0);
                    sourceNode.revertCompleteLatency(estimateT * 1000.0 * adjRatio);
                    break;
                } else {
                    if ((estimateT * 1000.0 * adjRatio) > completeTimeMilliSecUpper) {
                        highMark = lambda0;
                        lambda0 = (lowMark + highMark) / 2;
                    } else {
                        lowMark = lambda0;
                        lambda0 = (lowMark + highMark) / 2;
                    }
                }
            }
            if(lambda0 < 1) {
                System.out.println("high:" + highMark + " low:" + lowMark);
                System.out.println("gaonidayedemao"+lambda0);
//                for (ServiceNode serviceNode : serviceNodes.values()) {
//                    serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(), lambda0);
//                }
                return null;
            }
            System.out.println("high:" + highMark + " low:" + lowMark);
            System.out.println("final lambda0: " + lambda0 + " rate: " + (1 - (lambda0 / originLambda0)));
            activeShedRateMap.put(sourceNode.getComponentID(), (1 - (lambda0 / originLambda0)));
            return new NewAllocationAndActiveShedRates(tempAllocation, activeShedRateMap);
        }
        return null;
//        if (lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec) {
//            LOG.debug(" getMinReqServerAllocationGeneralTopApplyMMK(), " +
//                    "lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec");
//            double highMark = sourceNode.getTupleEmitRateOnSQ();
//            double lowMark = 0.0;
//            double lambda0 = (lowMark + highMark) / 2;
//            double estTime = 0.0;
//            while (lambda0 > 1 && highMark > lowMark) {// when infeasible out
//                int i = currentUsedThreadByBolts + reUnit;
//                for (; i <= maxAvailableExec; i += reUnit) {
//                    currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, i);
//                    double currTime = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation);
//
//                    LOG.debug(String.format("completeT upper bound (ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
//                            completeTimeMilliSecUpper, currTime * 1000.0, currTime * 1000.0 * adjRatio, i));
//                    System.out.println(currAllocation+" time: "+(currTime * 1000.0 * adjRatio)+":bupanijiao:"+completeTimeMilliSecUpper+"~"+completeTimeMilliSecLower);
//                    if (currTime * 1000.0 * adjRatio < completeTimeMilliSecUpper) {
//                        estTime = currTime * 1000.0 * adjRatio;
//                        break;
//                    }
//                }
//
//                if (i > maxAvailableExec) {
//                    LOG.info("adjustment allocation more than the max available executor: "+i+" > "+maxAvailableExec);
//                    highMark = lambda0;
//                    lambda0 = (lowMark + highMark) / 2;
//                } else {
//                    if (estTime < completeTimeMilliSecUpper && estTime / completeTimeMilliSecUpper > relativeE) {
//                        System.out.println(currAllocation+"chenggongadjust: "+activeShedRateMap);
//                        activeShedRateMap.put(sourceNode.getComponentID(), 0.0);
//                        return new NewAllocationAndActiveShedRates(currAllocation, activeShedRateMap);
//                    } else {
//                        if ((estTime) > completeTimeMilliSecUpper) {
//                            highMark = lambda0;
//                            lambda0 = (lowMark + highMark) / 2;
//                        } else {
//                            lowMark = lambda0;
//                            lambda0 = (lowMark + highMark) / 2;
//                        }
//                    }
//                }
//            }
//            System.out.println(lowMark+"~"+highMark+"buchenggong"+lambda0);
//            return null;
//        } else {
//            System.out.println("shibai"+(lowerBoundServiceTimeSeconds * adjRatio * 1000.0)+"~"+completeTimeMilliSecUpper+"~"+totalMinReq+"~"+maxAvailableExec);
//            return null;
//        }
    }

    public static Map<String, Integer> getRemovedAllocationWithAdjRatioGeneralTopApplyMMK(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, Map<String, ServiceNode> serviceNodes,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower, int currentUsedThreadByBolts, int reUnit, double adjRatio) {

        int totalMinReq2 = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            totalMinReq2 += getMinReqServerCount(lambda, mu);
        }

        Map<String, Integer>  minPossibleAllocation = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int totalMinReq = minPossibleAllocation.values().stream().mapToInt(Integer::intValue).sum();

        if (totalMinReq != totalMinReq2){
            LOG.warn(" getMinReqServerAllocationGeneralTopApplyMMK(), totalMinReq (" + totalMinReq + ") != totalMinReq2 (" + totalMinReq2 + ").");
        }

        if (adjRatio == 0) {
            adjRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        }

        Map<String, Integer> currAllocation = null;
        if (currentUsedThreadByBolts > totalMinReq){
            LOG.debug(" In getRemovedAllocationGeneralTopApplyMMK(), currentUsedThreadByBolts > totalMinReq");
            int i = currentUsedThreadByBolts - reUnit;
            for (; i > totalMinReq; i = i - reUnit) {
                currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, i);
                double currTime = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation);

                LOG.debug(String.format(" completeT lower bound (ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        completeTimeMilliSecLower, currTime * 1000.0, currTime * 1000.0 * adjRatio, i));
                if (currTime * 1000.0 * adjRatio > completeTimeMilliSecLower) {
                    break;
                }
            }
            if (i > totalMinReq) {
                return currAllocation;
            }
        }

        return minPossibleAllocation;
    }


    /**
     * Like Module A', input required QoS, output #threads required
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param realLatencyMilliSeconds
     * @param estTotalSojournTimeMilliSec_MMK
     * @param serviceNodes
     * @param completeTimeMilliSecUpper
     * @param completeTimeMilliSecLower
     * @param currentUsedThreadByBolts
     * @param maxAvailableExec
     * @return null when status is INFEASIBLE; or FEASIBLE reallocation (with resource added)
     */
    static Map<String, Integer> getMinReqServerAllocationGeneralTopApplyMMK(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, Map<String, ServiceNode> serviceNodes,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower, int currentUsedThreadByBolts, int maxAvailableExec, int reUnit) {

        double lowerBoundServiceTimeSeconds = 0.0;  //in seconds
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            ///caution, the unit should be millisecond
            lowerBoundServiceTimeSeconds += (1.0 / mu);
            totalMinReq += getMinReqServerCount(lambda, mu);
        }

        double adjRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);

        Map<String, Integer> currAllocation = null;
        if (lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec) {
            LOG.debug(" getMinReqServerAllocationGeneralTopApplyMMK(), " +
                    "lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec");

            int i = currentUsedThreadByBolts + reUnit;
            for (; i <= maxAvailableExec; i += reUnit) {
                currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, i);
                double currTime = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation);

                LOG.debug(String.format("completeT upper bound (ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        completeTimeMilliSecUpper, currTime * 1000.0, currTime * 1000.0 * adjRatio, i));
                System.out.println(currAllocation+" time: "+(currTime * 1000.0 * adjRatio)+":bupanijiao:"+completeTimeMilliSecUpper+"~"+completeTimeMilliSecLower);
                if (currTime * 1000.0 * adjRatio < completeTimeMilliSecUpper) {
                    break;
                }
            }

            if (i <= maxAvailableExec) {
                return currAllocation;
            }
        }
        return null;
    }

    public static Map<String, Integer> getRemovedAllocationGeneralTopApplyMMK(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, Map<String, ServiceNode> serviceNodes,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower, int currentUsedThreadByBolts, int reUnit) {

        int totalMinReq2 = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            totalMinReq2 += getMinReqServerCount(lambda, mu);
        }

        Map<String, Integer>  minPossibleAllocation = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int totalMinReq = minPossibleAllocation.values().stream().mapToInt(Integer::intValue).sum();

        if (totalMinReq != totalMinReq2){
            LOG.warn(" getMinReqServerAllocationGeneralTopApplyMMK(), totalMinReq (" + totalMinReq + ") != totalMinReq2 (" + totalMinReq2 + ").");
        }

        double adjRatio = realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK;

        Map<String, Integer> currAllocation = null;
        if (currentUsedThreadByBolts > totalMinReq){
            LOG.debug(" In getRemovedAllocationGeneralTopApplyMMK(), currentUsedThreadByBolts > totalMinReq");
            int i = currentUsedThreadByBolts - reUnit;
            for (; i > totalMinReq; i = i - reUnit) {
                currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, i);
                double currTime = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation);

                LOG.debug(String.format("completeT lower bound (ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        completeTimeMilliSecLower, currTime * 1000.0, currTime * 1000.0 * adjRatio, i));
                if (currTime * 1000.0 * adjRatio > completeTimeMilliSecLower) {
                    break;
                }
            }
            if (i > totalMinReq) {
                return currAllocation;
            }
        }

        return minPossibleAllocation;
    }

    /**
     * Created by Tom Fu on Feb 21, 2017, for ToN Major revision-1, to enable resource adjustment and auto-reallocation
     * Three cases for consideration:
     * 1) resource over-provision, i.e., too much resources are used, and the real latency is far below the allowed bound
     * 2) resource shortage, i.e., resource is not enough , hence the real latency is beyond the upper-bound
     * 3) resource proper, case a) resource is just fine, only need to check whether it is in good allocation.
     * case b) the current allocation is bad, however, after reallocation to optimal allocation,
     * it will be below upper-bound
     *
     * @param realLatencyMilliSeconds
     * @param estTotalSojournTimeMilliSec_MMK
     * @param serviceNodes
     * @param completeTimeMilliSecUpper
     * @param completeTimeMilliSecLower,
     * @return AllocResult.Status
     */
    public static AllocResult.Status getStatusMMK(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, double estTotalSojournTimeMilliSec_MMKOpt,
            Map<String, ServiceNode> serviceNodes, double completeTimeMilliSecUpper, double completeTimeMilliSecLower) {

        double ratio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        if (realLatencyMilliSeconds < completeTimeMilliSecLower) {
            return AllocResult.Status.OVERPROVISIONING;
        } else if (realLatencyMilliSeconds > completeTimeMilliSecUpper
                && ratio * estTotalSojournTimeMilliSec_MMKOpt > completeTimeMilliSecUpper) {

            //TODO: Here we conservatively include the case that the when "realLatencyMilliSeconds > completeTimeMilliSecUpper",
            //TODO: but current allocation is not the optimal one, then we will consider try optimal one before add more resources.
            return AllocResult.Status.SHORTAGE;
        }
        return AllocResult.Status.FEASIBLE;
    }

    @Override
    public ShedRateAndAllocResult checkOptimized(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                                 double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
                                                 Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
                                                 int currentUsedThreadByBolts, int resourceUnit) {

        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, currentUsedThreadByBolts);
        double estTotalSojournTimeMilliSec_MMKOpt = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currOptAllocation);
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currBoltAllocation);

        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        AllocResult.Status status = getStatusMMK(realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, estTotalSojournTimeMilliSec_MMKOpt,
                queueingNetwork, completeTimeMilliSecUpper, completeTimeMilliSecLower);

        Map<String, Integer> adjustedAllocation = null;
        if (status.equals(AllocResult.Status.SHORTAGE)) {
            LOG.debug("Status is resource shortage, calling resource adjustment ");
            adjustedAllocation = getMinReqServerAllocationGeneralTopApplyMMK(
                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, maxAvailable4Bolt, resourceUnit);
            if (adjustedAllocation == null){
                LOG.debug(" Status is resource shortage and no feasible re-allocation solution");
                status = AllocResult.Status.INFEASIBLE;
            }

        } else if (status.equals(AllocResult.Status.OVERPROVISIONING)) {
            LOG.debug("Status is resource over-provisioning");
            adjustedAllocation = getRemovedAllocationGeneralTopApplyMMK(
                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, resourceUnit);
        }

        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMK", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMK", underEstimateRatio);
        context.put("reMMK", relativeError);

        LOG.info(String.format("realLatency(ms): %.4f, estMMK: %.4f, urMMK: %.4f, reMMK: %.4f, status: %s",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError, status.toString()));

        Map<String, Map<String,Double>> activeSheddingRateMap = calcActiveSheddingRate();//active shedding

        ShedRateAndAllocResult shedRateAndAllocResult = new ShedRateAndAllocResult(status, adjustedAllocation, currOptAllocation, kMaxOptAllocation,activeSheddingRateMap,context);
        AllocResult retVal = shedRateAndAllocResult.getAllocResult();

        LOG.info("MMK, reUnit: " + resourceUnit  +  ", alloStat: " + retVal.status);
        LOG.info("MMK, currOptAllo: " + retVal.currOptAllocation);
        LOG.info("MMK, adjustAllo: " + retVal.minReqOptAllocation);
        LOG.info("MMK, kMaxOptAllo: " + retVal.kMaxOptAllocation);

        return shedRateAndAllocResult;
    }

    @Override
    public ShedRateAndAllocResult checkOptimizedWithShedding(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                                 double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
                                                 Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
                                                 int currentUsedThreadByBolts, int resourceUnit, double relativeE,
                                                 double messageTimeOut, Map<String, double[]> selectivityFunctions, Map<String,Object> targets) {
        double activeShedRate;
        double adjRatio;
        Map<String, Map<String,Double>> activeSheddingRateMap = new HashMap<>();
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        //System.out.println(sourceNode.getRealLatencySeconds()+"simalaofu: "+sourceNode.getTupleEmitRateOnSQ());
        //System.out.println("simalangbie: "+((HashMap)queueingNetwork).toString());

        NewAllocationAndActiveShedRates KmaxOptAllocationAndActiveShedRates = suggestAllocationWithShedRate(sourceNode,queueingNetwork,maxAvailable4Bolt,completeTimeMilliSecUpper,relativeE,selectivityFunctions, targets, currBoltAllocation);//(messageTimeOut*1000)
        activeShedRate = KmaxOptAllocationAndActiveShedRates.getActiveShedRates().get(sourceNode.getComponentID());
        activeSheddingRateMap.put("KmaxActiveShedRate",KmaxOptAllocationAndActiveShedRates.getActiveShedRates());

        System.out.println("11111dkmax activeShedRate: "+activeShedRate);

        NewAllocationAndActiveShedRates currOptAllocationAndActiveShedRates = suggestAllocationWithShedRate(sourceNode,queueingNetwork,currentUsedThreadByBolts,completeTimeMilliSecUpper,relativeE,selectivityFunctions, targets, currBoltAllocation);

        System.out.println("11111dcurr activeShedRate: "+currOptAllocationAndActiveShedRates.getActiveShedRates().get(sourceNode.getComponentID()));
        //Map<String, Integer> temp = newAllocationAndActiveShedRates.getFixedAllocation();
        //System.out.println(sourceNode.getRealLatencySeconds()+"simafu: "+sourceNode.getTupleEmitRateOnSQ());
        //System.out.println("simalang: "+((HashMap)queueingNetwork).toString());

        activeShedRate = //activeShedRate > currOptAllocationAndActiveShedRates.getActiveShedRates().get(sourceNode.getComponentID()) ? activeShedRate : currOptAllocationAndActiveShedRates.getActiveShedRates().get(sourceNode.getComponentID());
                1 - ((1-activeShedRate)*(1-currOptAllocationAndActiveShedRates.getActiveShedRates().get(sourceNode.getComponentID())));
        currOptAllocationAndActiveShedRates.getActiveShedRates().put(sourceNode.getComponentID(),activeShedRate);
        activeSheddingRateMap.put("currOptActiveShedRate",currOptAllocationAndActiveShedRates.getActiveShedRates());

        System.out.println("final shed rate: "+activeShedRate);

        Map<String, Integer> currOptAllocation = currOptAllocationAndActiveShedRates.getFixedAllocation();
        Map<String, Integer> kMaxOptAllocation = KmaxOptAllocationAndActiveShedRates.getFixedAllocation();

        //Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, maxAvailable4Bolt);
        //Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, currentUsedThreadByBolts);

        System.out.println(currOptAllocation+"currOptAllocation : simafang : kMaxOptAllocation "+kMaxOptAllocation);

        double estTotalSojournTimeMilliSec_MMKOpt = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currOptAllocation);
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currBoltAllocation);
        System.out.println(estTotalSojournTimeMilliSec_MMKOpt+"estTotalSojournTimeMilliSec_MMKOpt : heyingli : estTotalSojournTimeMilliSec_MMK "+estTotalSojournTimeMilliSec_MMK);

        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);

        if (estTotalSojournTimeMilliSec_MMK < 0.0) {
            System.out.println("yohoho"+(realLatencyMilliSeconds / 1000.0));
            paramPairForCalcAdjRatio.putResult(realLatencyMilliSeconds / 1000.0, realLatencyMilliSeconds / 1000.0);
        } else {
            System.out.println((estTotalSojournTimeMilliSec_MMK / 1000.0)+"yoheihei"+(realLatencyMilliSeconds / 1000.0));
            paramPairForCalcAdjRatio.putResult(realLatencyMilliSeconds / 1000.0, estTotalSojournTimeMilliSec_MMK / 1000.0);
        }
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        AllocResult.Status status = activeShedRate > 0.1 ? AllocResult.Status.SHORTAGE : getStatusMMK(realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, estTotalSojournTimeMilliSec_MMKOpt,
                queueingNetwork, completeTimeMilliSecUpper, completeTimeMilliSecLower);

        Map<String, Integer> adjustedAllocation = null;
//        if (status.equals(AllocResult.Status.SHORTAGE)) {
//            LOG.debug("Status is resource shortage, calling resource adjustment ");
//            adjustedAllocation = getMinReqServerAllocationGeneralTopApplyMMK(
//                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
//                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, maxAvailable4Bolt, resourceUnit);
//            if (adjustedAllocation == null) {
//                LOG.debug("Status is resource shortage and no feasible re-allocation solution");
//                status = AllocResult.Status.INFEASIBLE;
//            }
        if (!paramPairForCalcAdjRatio.historyAdjustRatioResults.isEmpty()) {
            PolynomialRegression polynomialRegression = new PolynomialRegression();
            Object[] objects = new Object[2];
            objects[0] = paramPairForCalcAdjRatio.historyAdjustRatioResults;
            objects[1] = 1;
            double[] adjustRatioArr = polynomialRegression.Fit(objects);
            adjRatio = adjustRatioArr[adjustRatioArr.length - 1];// > 1 ? adjustRatioArr[adjustRatioArr.length - 1]:1;
            System.out.println(adjustRatioArr[adjustRatioArr.length - 1]+"~"+paramPairForCalcAdjRatio.historyAdjustRatioResults.getFirst()+"~"+paramPairForCalcAdjRatio.historyAdjustRatioResults.size()+"ahuadaxiabi: "+adjRatio+"~~"+sourceNode.getRealLatencySeconds());
        } else {
            adjRatio = 0;
            System.out.println(paramPairForCalcAdjRatio.historyAdjustRatioResults.size()+"ahuadajiba: "+adjRatio);
        }

        if (status.equals(AllocResult.Status.SHORTAGE)) {
            LOG.debug("Status is resource shortage, calling resource adjustment ");
            //suggestAllocationWithShedRate(sourceNode,queueingNetwork,currentUsedThreadByBolts,completeTimeMilliSecUpper,relativeE,selectivityFunctions, targets);
            NewAllocationAndActiveShedRates adjustedAllocationAndShedRate = getMinReqServerAllocationAndShedRateGeneralTopApplyMMK(sourceNode, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, maxAvailable4Bolt, relativeE, currOptAllocation,adjRatio);
            if (adjustedAllocationAndShedRate != null) {
                adjustedAllocation = adjustedAllocationAndShedRate.getFixedAllocation();
                activeShedRate = 1 - ((1 - activeShedRate) * (1 - adjustedAllocationAndShedRate.getActiveShedRates().get(sourceNode.getComponentID())));
                adjustedAllocationAndShedRate.getActiveShedRates().put(sourceNode.getComponentID(),activeShedRate);
                activeSheddingRateMap.put("adjustedActiveShedRate",adjustedAllocationAndShedRate.getActiveShedRates());
            } else {
                adjustedAllocation = null;
                activeSheddingRateMap.put("adjustedActiveShedRate",null);
            }
            //activeShedRate = adjustedAllocationAndShedRate.getActiveShedRates().get(sourceNode.getComponentID());
            if (adjustedAllocation == null) {
                LOG.debug("Status is resource shortage and no feasible re-allocation solution");
                status = AllocResult.Status.INFEASIBLE;
            }

        } else if (status.equals(AllocResult.Status.OVERPROVISIONING)) {
            LOG.debug("Status is resource over-provisioning");
            adjustedAllocation = getRemovedAllocationWithAdjRatioGeneralTopApplyMMK(
                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, resourceUnit, adjRatio);
            activeSheddingRateMap.put("adjustedActiveShedRate",null);
        }

        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMK", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMK", underEstimateRatio);
        context.put("reMMK", relativeError);

        LOG.info(String.format("realLatency(ms): %.4f, estMMK: %.4f, urMMK: %.4f, reMMK: %.4f, status: %s",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError, status.toString()));

        //Map<String,Double> activeSheddingRateMap = calcActiveSheddingRate();//active shedding

        ShedRateAndAllocResult shedRateAndAllocResult = new ShedRateAndAllocResult(status, adjustedAllocation, currOptAllocation, kMaxOptAllocation,activeSheddingRateMap,context);
        AllocResult retVal = shedRateAndAllocResult.getAllocResult();

        LOG.info("MMK, reUnit: " + resourceUnit  +  ", alloStat: " + retVal.status);
        LOG.info("MMK, currOptAllo: " + retVal.currOptAllocation);
        LOG.info("MMK, adjustAllo: " + retVal.minReqOptAllocation);
        LOG.info("MMK, kMaxOptAllo: " + retVal.kMaxOptAllocation);

        return shedRateAndAllocResult;
    }

    @Override
    public ShedRateAndAllocResult checkOptimizedWithActiveShedding(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork, double completeTimeMilliSecUpper, double completeTimeMilliSecLower, Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt, int currentUsedThreadByBolts, int resourceUnit, double relativeE, double messageTimeOut, Map<String, double[]> selectivityFunctions, Map<String, Object> targets) {

        double activeShedRate;
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, currentUsedThreadByBolts);
        double estTotalSojournTimeMilliSec_MMKOpt = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currOptAllocation);
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currBoltAllocation);

        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        AllocResult.Status status = getStatusMMK(realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, estTotalSojournTimeMilliSec_MMKOpt,
                queueingNetwork, completeTimeMilliSecUpper, completeTimeMilliSecLower);

        Map<String, Integer> adjustedAllocation = null;
        if (status.equals(AllocResult.Status.SHORTAGE)) {
            LOG.debug("Status is resource shortage, calling resource adjustment ");
            //suggestAllocationWithShedRate(sourceNode,queueingNetwork,currentUsedThreadByBolts,completeTimeMilliSecUpper,relativeE,selectivityFunctions, targets);
            NewAllocationAndActiveShedRates adjustedAllocationAndShedRate = getMinReqServerAllocationAndShedRateGeneralTopApplyMMK(sourceNode, estTotalSojournTimeMilliSec_MMKOpt, queueingNetwork,
            completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, maxAvailable4Bolt, relativeE, currOptAllocation, 1);
            if (adjustedAllocationAndShedRate != null) {
                adjustedAllocation = adjustedAllocationAndShedRate.getFixedAllocation();
            } else {
                adjustedAllocation = null;
            }
            //activeShedRate = adjustedAllocationAndShedRate.getActiveShedRates().get(sourceNode.getComponentID());
            if (adjustedAllocation == null) {
                LOG.debug("Status is resource shortage and no feasible re-allocation solution");
                status = AllocResult.Status.INFEASIBLE;
            }

        } else if (status.equals(AllocResult.Status.OVERPROVISIONING)) {
            LOG.debug(" Status is resource over-provisioning");
            adjustedAllocation = getRemovedAllocationGeneralTopApplyMMK(
                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, resourceUnit);
        }

        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMK", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMK", underEstimateRatio);
        context.put("reMMK", relativeError);

        LOG.info(String.format("realLatency(ms): %.4f, estMMK: %.4f, urMMK: %.4f, reMMK: %.4f, status: %s",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError, status.toString()));

        Map<String, Map<String,Double>> activeSheddingRateMap = calcActiveSheddingRate();//active shedding

        ShedRateAndAllocResult shedRateAndAllocResult = new ShedRateAndAllocResult(status, adjustedAllocation, currOptAllocation, kMaxOptAllocation,activeSheddingRateMap,context);
        AllocResult retVal = shedRateAndAllocResult.getAllocResult();

        LOG.info("MMK, reUnit: " + resourceUnit  +  ", alloStat: " + retVal.status);
        LOG.info("MMK, currOptAllo: " + retVal.currOptAllocation);
        LOG.info("MMK, adjustAllo: " + retVal.minReqOptAllocation);
        LOG.info("MMK, kMaxOptAllo: " + retVal.kMaxOptAllocation);

        return shedRateAndAllocResult;
    }

    /**
     * if mu = 0.0 or serverCount not positive, then rho is not defined, we consider it as the unstable case (represented by Double.MAX_VALUE)
     * otherwise, return the calculation results. Leave the interpretation to the calling function, like isStable();
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    private static double calcRho(double lambda, double mu, int serverCount) {
        return (mu > 0.0 && serverCount > 0) ? lambda / (mu * (double) serverCount) : Double.MAX_VALUE;
    }

    /**
     * First call getRho,
     * then determine when rho is validate, i.e., rho < 1.0
     * otherwise return unstable (FALSE)
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static boolean isStable(double lambda, double mu, int serverCount) {
        return calcRho(lambda, mu, serverCount) < 1.0;
    }

    private static double calcRhoSingleServer(double lambda, double mu) {
        return calcRho(lambda, mu, 1);
    }

    public static int getMinReqServerCount(double lambda, double mu) {
        return (int) (lambda / mu) + 1;
    }

    /**
     * we assume the stability check is done before calling this function
     * The total sojournTime of an MMK queue is the sum of queueing time and expected service time (1.0 / mu).
     *
     * @param lambda,     average arrival rate
     * @param mu,         average execute rate
     * @param serverCount
     * @return
     */
    public static double sojournTime_MMK(double lambda, double mu, int serverCount) {
        return avgQueueingTime_MMK(lambda, mu, serverCount) + 1.0 / mu;
    }
    /**
     * we assume the stability check is done before calling this function
     * This is a standard erlang-C formula
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static double avgQueueingTime_MMK(double lambda, double mu, int serverCount) {
        double r = lambda / (mu * (double) serverCount);
        double kr = lambda / mu;

        double phi0_p1 = 1.0;
        for (int i = 1; i < serverCount; i++) {
            double a = Math.pow(kr, i);
            double b = (double) factorial(i);
            phi0_p1 += (a / b);
        }

        double phi0_p2_nor = Math.pow(kr, serverCount);
        double phi0_p2_denor = (1.0 - r) * (double) (factorial(serverCount));
        double phi0_p2 = phi0_p2_nor / phi0_p2_denor;

        double phi0 = 1.0 / (phi0_p1 + phi0_p2);

        double pWait = phi0_p2 * phi0;

        double waitingTime = pWait * r / ((1.0 - r) * lambda);

        return waitingTime;
    }

    public static double sojournTime_MM1(double lambda, double mu) {
        return 1.0 / (mu - lambda);
    }

    private static int factorial(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("Attention, negative input is not allowed: " + n);
        } else if (n == 0) {
            return 1;
        } else {
            int ret = 1;
            for (int i = 2; i <= n; i++) {
                ret = ret * i;
            }
            return ret;
        }
    }

    private Map<String, Map<String,Double>> calcActiveSheddingRate() {
        Map<String, Map<String,Double>> test = new HashMap<>();
        Map<String,Double> activeSheddingRateMap = new HashMap<>();
        activeSheddingRateMap.put("sort-BoltB",Math.random());
        activeSheddingRateMap.put("sort-BoltC",Math.random());
        activeSheddingRateMap.put("sort-BoltD",0.5);
        test.put("kmax",activeSheddingRateMap);
        return test;
    }
}
