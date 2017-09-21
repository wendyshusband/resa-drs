package resa.shedding.basicServices;

import resa.optimize.ServiceNode;
import resa.optimize.SourceNode;
import resa.shedding.basicServices.api.ICostFunction;
import resa.shedding.basicServices.api.LearningModel;

import java.util.Map;

/**
 * Created by Tom.fu on May-8-2017
 */
public interface SheddingServiceModel {

    /**
     * A general interface for service models (e.g., MMK, GGK) for Storm 1.0.1, including two thresholds
     *
     * @param sourceNode
     * @param queueingNetwork
     * @param completeTimeMilliSecUpper i.e., T_{Max}
     * @param completeTimeMilliSecLower i.e., T_{Min}
     * @param currBoltAllocation
     * @param maxAvailable4Bolt         i.e., k_{Max}
     * @param currentUsedThreadByBolts  i.e., sum of number of executors in @currBoltAllocation
     * @param resourceUnit              used by get Min/MaxAllocation, each time the minimum number of executors will be added or removed
     * @return
     */
    ShedRateAndAllocResult checkOptimized(
            SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
            Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
            int currentUsedThreadByBolts, int resourceUnit);

    ShedRateAndAllocResult checkOptimizedWithShedding(
            SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
            Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
            int currentUsedThreadByBolts, int resourceUnit, double tolerant,
            double messageTimeOut, Map<String, double[]> selectivityFunctions, LearningModel calcAdjRatioFunction,
            Map<String,Object> targets, Map<String, RevertRealLoadData> revertRealLoadDatas, ICostFunction costFunction, String costClassName, int systemModel);

    ShedRateAndAllocResult checkOptimizedWithActiveShedding(
            SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
            Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
            int currentUsedThreadByBolts, int resourceUnit, double tolerant,
            double messageTimeOut, Map<String, double[]> selectivityFunctions,  Map<String,Object> targets);
}
