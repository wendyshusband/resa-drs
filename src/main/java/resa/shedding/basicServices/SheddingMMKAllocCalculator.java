package resa.shedding.basicServices;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.utils.Utils;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.optimize.*;
import resa.shedding.basicServices.api.ICostFunction;
import resa.shedding.basicServices.api.LearningModel;
import resa.shedding.basicServices.api.SheddingAllocCalculator;
import resa.shedding.example.ExamCost;
import resa.shedding.example.ExamCostFunc;
import resa.shedding.example.PolynomialRegression;
import resa.shedding.tools.DRSzkHandler;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.ResaUtils;

import java.util.*;
import java.util.stream.Collectors;

import static resa.util.ResaConfig.SHEDDING_SERVICE_MODEL_CLASS;

/**
 * Created by kailin on 13/4/17.
 */
public class  SheddingMMKAllocCalculator extends SheddingAllocCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(SheddingMMKAllocCalculator.class);
    private HistoricalCollectedData spoutHistoricalData;
    private HistoricalCollectedData boltHistoricalData;
    private int historySize;
    private int currHistoryCursor;
    private SheddingServiceModel serviceModel;
    private LearningModel calcSelectivityFunction;
    private Integer order;
    private Map<String,Integer> selectivityOrder = new HashMap<>();
    private boolean enablePassiveShedding;
    private boolean enableActiveShedding;

    private transient CuratorFramework client;
    private String topologyName;

    @Override
    public void init(Map<String, Object> conf, Map<String, Integer> currAllocation, StormTopology rawTopology, Map<String, Object> targets) {
        super.init(conf, currAllocation, rawTopology, targets);
        ///The first (historySize - currHistoryCursor) window data will be ignored.
        historySize = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_WIN_HISTORY_SIZE, 1);
        currHistoryCursor = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_WIN_HISTORY_SIZE_IGNORE, 0);
        spoutHistoricalData = new HistoricalCollectedData(rawTopology, historySize);
        boltHistoricalData = new HistoricalCollectedData(rawTopology, historySize);
        serviceModel =  ResaUtils.newInstanceThrow((String) conf.getOrDefault(SHEDDING_SERVICE_MODEL_CLASS,
                SheddingMMKServiceModel.class.getName()), SheddingServiceModel.class);
        calcSelectivityFunction = ResaUtils.newInstanceThrow(ConfigUtil.getString(conf, ResaConfig.SELECTIVITY_CALC_CLASS,
                PolynomialRegression.class.getName()),LearningModel.class);
        order = ConfigUtil.getInt(conf, ResaConfig.SELECTIVITY_FUNCTION_ORDER,1);
        currAllocation.keySet().stream().forEach(e->{
            selectivityOrder.put(e,order);
        });
        enablePassiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.PASSIVE_SHEDDING_ENABLE, true);
        enableActiveShedding = ConfigUtil.getBoolean(conf, ResaConfig.ACTIVE_SHEDDING_ENABLE, true);
        if (enableActiveShedding) {
            List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
            int port = Math.toIntExact((long) conf.get(Config.STORM_ZOOKEEPER_PORT));
            client = DRSzkHandler.newClient(zkServer.get(0).toString(), port, 6000, 6000, 1000, 3);
            topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        }
    }

    @Override
    public ShedRateAndAllocResult calc(Map<String, AggResult[]> executorAggResults, int maxAvailableExecutors,
                            StormTopology topology, Map<String, Object> targets) {
        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .forEach(e -> spoutHistoricalData.putResult(e.getKey(), e.getValue()));
        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .forEach(e -> boltHistoricalData.putResult(e.getKey(), e.getValue()));
        // check history size. Ensure we have enough history data before we run the optimize function
        currHistoryCursor++;
        if (currHistoryCursor < historySize) {
            LOG.info("currHistoryCursor < historySize, curr: " + currHistoryCursor + ", Size: " + historySize
                    + ", DataHistorySize: "
                    + spoutHistoricalData.compHistoryResults.entrySet().stream().findFirst().get().getValue().size());
            return null;
        } else {
            currHistoryCursor = historySize;
        }

        Map<String,double[]> selectivityFunctions = calcSelectivityFunction();//load shedding
        ///TODO: Here we assume only one spout, plan to extend to multiple spouts in future
        ///TODO: here we assume only one running topology, plan to extend to multiple running topologies in future
        double targetQoSMs = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_QOS_MS, 5000.0);
        double completeTimeMilliSecUpper = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_QOS_UPPER_MS, 2000.0);
        double completeTimeMilliSecLower = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_QOS_LOWER_MS, 500.0);
        int maxSendQSize = ConfigUtil.getInt(conf, Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 1024);
        int maxRecvQSize = ConfigUtil.getInt(conf, Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        double sendQSizeThresh = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_SEND_QUEUE_THRESH, 5.0);
        double recvQSizeThreshRatio = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_RECV_QUEUE_THRESH_RATIO, 0.6);
        double recvQSizeThresh = recvQSizeThreshRatio * maxRecvQSize;
        int resourceUnit = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_SMD_RESOURCE_UNIT,1);
        int messageTimeOut = Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        int systemModel = ConfigUtil.getInt(conf, ResaConfig.SHED_SYSTEM_MODEL,1);
        double tolerant = ConfigUtil.getDouble(conf, ResaConfig.ACTIVE_SHEDDING_ADJUSTRATIO_BIAS_THRESHOLD,0.9);
        LearningModel calcAdjRatioFunction = ResaUtils.newInstanceThrow(ConfigUtil.getString(conf, ResaConfig.ADJRATIO_CALC_CLASS,
                PolynomialRegression.class.getName()),LearningModel.class);
        ICostFunction costFunction = ResaUtils.newInstanceThrow(ConfigUtil.getString(conf, ResaConfig.CALC_COST_FUNC_CLASS,
                ExamCostFunc.class.getName()),ICostFunction.class);
        String costClassName =  ConfigUtil.getString(conf, ResaConfig.COST_CLASS, ExamCost.class.getName());

        ShedRateAndAllocResult shedRateAndAllocResult;

        ///TODO: check how metrics are sampled in the current implementation.
        double componentSampelRate = ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 1.0);

        //Map<String, Map<String, Object>> queueMetric = new HashMap<>();
        Map<String, SourceNode> spInfos = spoutHistoricalData.compHistoryResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    SpoutAggResult hisCar = AggResult.getHorizontalCombinedResult(new SpoutAggResult(), e.getValue());
                    int numberExecutor = currAllocation.get(e.getKey());
                    System.out.println("spoutaggresultguibian: "+hisCar.getShedRelateCount());
                    return new SourceNode(e.getKey(), numberExecutor, componentSampelRate, hisCar, true);
                }));

        SourceNode spInfo = spInfos.entrySet().stream().findFirst().get().getValue();
        Map<String, ServiceNode> queueingNetwork = boltHistoricalData.compHistoryResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    BoltAggResult hisCar = AggResult.getHorizontalCombinedResult(new BoltAggResult(), e.getValue());
                    int numberExecutor = currAllocation.get(e.getKey());
                    ///TODO: here i2oRatio can be INFINITY, when there is no data sent from Spout.
                    ///TODO: here we shall deside whether to use external Arrival rate, or tupleLeaveRateOnSQ!!
                    ///TODO: major differences 1) when there is max-pending control, tupleLeaveRateOnSQ becomes the
                    ///TODO: the tupleEmit Rate, rather than the external tuple arrival rate (implicit load shading)
                    ///TODO: if use tupleLeaveRateOnSQ(), be careful to check if ACKing mechanism is on, i.e.,
                    ///TODO: there are ack tuples. othersize, devided by tow becomes meaningless.
                    ///TODO: shall we put this i2oRatio calculation here, or later to inside ServiceModel?
                    return new ServiceNode(e.getKey(), numberExecutor, componentSampelRate, hisCar, spInfo.getExArrivalRate());
                }));

        SheddingLoadRevert sheddingLoadRevert = new SheddingLoadRevert(conf,spInfo,queueingNetwork,rawTopology,targets,selectivityFunctions);//load shedding
        sheddingLoadRevert.revertLoad();
        System.out.println("****************************test*************************************");
        for  (RevertRealLoadData data : sheddingLoadRevert.getRevertRealLoadDatas().values()) {
            System.out.println(data);
        }
        System.out.println("*********************************************************************");
        Map<String, Integer> boltAllocation = currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        /** totalAvailableExecutors - spoutExecutors, currently, it is assumed that there is only one spout **/
        int maxThreadAvailable4Bolt = maxAvailableExecutors - currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .mapToInt(Map.Entry::getValue).sum();
        System.out.println(maxThreadAvailable4Bolt+"woyaobengkuile:"+maxAvailableExecutors);
        int currentUsedThreadByBolts = currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_bolts().containsKey(e.getKey())).mapToInt(Map.Entry::getValue).sum();

        LOG.info("Run Optimization, tQos: " + targetQoSMs + ", currUsed: " + currentUsedThreadByBolts + ", kMax: " + maxThreadAvailable4Bolt + ", currAllo: " + currAllocation);

        if (enableActiveShedding) {
            //shedRateAndAllocResult = serviceModel.checkOptimizedWithActiveShedding(spInfo, queueingNetwork,
            //        completeTimeMilliSecUpper, completeTimeMilliSecLower, boltAllocation, maxThreadAvailable4Bolt, currentUsedThreadByBolts, resourceUnit, tolerant, messageTimeOut, selectivityFunctions, targets);
            shedRateAndAllocResult = serviceModel.checkOptimizedWithShedding(spInfo, queueingNetwork, completeTimeMilliSecUpper, completeTimeMilliSecLower,
                    boltAllocation, maxThreadAvailable4Bolt, currentUsedThreadByBolts, resourceUnit, tolerant, messageTimeOut, selectivityFunctions,
                    calcAdjRatioFunction, targets, sheddingLoadRevert.getRevertRealLoadDatas(), costFunction, costClassName, systemModel);
        } else {
            shedRateAndAllocResult = serviceModel.checkOptimized(
                    spInfo, queueingNetwork, completeTimeMilliSecUpper, completeTimeMilliSecLower, boltAllocation, maxThreadAvailable4Bolt, currentUsedThreadByBolts, resourceUnit);
        }

        AllocResult allocResult = shedRateAndAllocResult.getAllocResult();
        Map<String, Map<String,Double>> activeShedRate = shedRateAndAllocResult.getActiveShedRate();
        Map<String, Integer> retCurrAllocation = null;
        if (allocResult.currOptAllocation != null) {
            retCurrAllocation = new HashMap<>(currAllocation);
            retCurrAllocation.putAll(allocResult.currOptAllocation);
        }
        Map<String, Integer> retKMaxAllocation = null;
        if (allocResult.kMaxOptAllocation != null) {
            retKMaxAllocation = new HashMap<>(currAllocation);
            retKMaxAllocation.putAll(allocResult.kMaxOptAllocation);
        }
        Map<String, Integer> retMinReqAllocation = null;
        if (allocResult.minReqOptAllocation != null) {
            retMinReqAllocation = new HashMap<>(currAllocation);
            retMinReqAllocation.putAll(allocResult.minReqOptAllocation);
        }
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("latency", allocResult.getContext());
        ctx.put("spout", spInfo);
        ctx.put("bolt", queueingNetwork);
        return new ShedRateAndAllocResult(allocResult.status, retMinReqAllocation, retCurrAllocation, retKMaxAllocation,activeShedRate,ctx);
    }

    /**
     * load shedding
     * calculate selectivity function based on bolt history data.
     * */
    private Map<String, double[]> calcSelectivityFunction() {
//        spoutHistoricalData.compHistoryResults.entrySet().forEach(e->{
//            System.out.println(e.getKey());
//            Iterator iterator = ((Queue)e.getValue()).iterator();
//            while(iterator.hasNext()){
//                SpoutAggResult tempAggResult = (SpoutAggResult) iterator.next();
//                System.out.println("completeLatency: "+tempAggResult.getScvTupleCompleteLatency());
//                System.out.println(tempAggResult.getArrivalRatePerSec());
//                System.out.println("getTupleEmitRateOnSQ"+tempAggResult.getArrivalRatePerSec() * currAllocation.get(e.getKey()) / 2.0);
//            }
//        });
        Map<String, double[]> selectivityCoeffs = new HashMap<>();
        Map<String, Queue<AggResult>> compHistoryResults =boltHistoricalData.compHistoryResults;

        for (Map.Entry comp : compHistoryResults.entrySet()) {
            Iterator iterator = ((Queue)comp.getValue()).iterator();
            LinkedList<Pair<Double,Double>> loadPairList = new LinkedList<>();

            while (iterator.hasNext()) {
                BoltAggResult tempAggResult = (BoltAggResult) iterator.next();
                //double sheddingRate = 0.0;
                double loadIN = 0.0;
                double loadOUT = 0.0;
                if (tempAggResult.getPassiveSheddingCountMap().get("allTuple") != null &&
                        tempAggResult.getPassiveSheddingCountMap().get("allTuple") != 0) {
                    long loadTuple = tempAggResult.getPassiveSheddingCountMap().get("allTuple")
                            - tempAggResult.getPassiveSheddingCountMap().get("dropTuple");
                    if (loadTuple > 0) {
                        loadIN = loadTuple;
                        int emitSum = tempAggResult.getemitCount().values().stream().mapToInt(Number::intValue).sum();
                        if (emitSum != 0) {
                            loadOUT = emitSum;
                        }
                        //System.out.println(loadTuple+" ::~:: "+emitSum);
                    }
                }
                loadPairList.add(new Pair<>(loadIN,loadOUT));
            }

            double[] oneCompSelectivityCoeff = calcSelectivityFunction.Fit(loadPairList,order,false);
            selectivityCoeffs.put((String) comp.getKey(),oneCompSelectivityCoeff);
        }

        System.out.println("this is the selectivity coeff!!!!!??????????????!");
        for (Map.Entry comp : selectivityCoeffs.entrySet()) {
            System.out.println((String) comp.getKey());
            double[] value = (double[]) comp.getValue();
            for(int i=0; i<value.length; i++){
                System.out.println(String.valueOf(value[i]));
            }
        }
        System.out.println("_______________________________________");

        return selectivityCoeffs;
    }

    @Override
    public void allocationChanged(Map<String, Integer> newAllocation) {
        super.allocationChanged(newAllocation);
        spoutHistoricalData.clear();
        boltHistoricalData.clear();
        currHistoryCursor = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_WIN_HISTORY_SIZE_IGNORE, 0);
    }
}
