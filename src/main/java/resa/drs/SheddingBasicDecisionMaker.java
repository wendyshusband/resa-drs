package resa.drs;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.optimize.AllocResult;
import resa.shedding.basicServices.ShedRateAndAllocResult;
import resa.shedding.basicServices.api.ISheddingDecisionMaker;
import resa.shedding.tools.DRSzkHandler;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.util.HashMap;
import java.util.Map;

import static resa.util.ResaConfig.OPTIMIZE_INTERVAL;

/**
 * Created by ding on 14-6-20.
 */
public class SheddingBasicDecisionMaker implements ISheddingDecisionMaker {

    private static final Logger LOG = LoggerFactory.getLogger(SheddingBasicDecisionMaker.class);
    //private transient CuratorFramework client;
    private String topologyName;
    private Double testRatio;

    /**
     * In future, if RebalanceType has more general usage, we will consider to move it to the ResaConfig class
     * case 1: return "MaxExecutorOpt";    ///always use up k_max number of executors
     * case 2: return "TwoThreshold";      ///ensure the expected sojourn time is between [T_min, T_max],
     * otherwise, it will automatically add/remove resources,
     * if amount of resources are appropriate, it checks wheter the current allocation is optimal or not.
     */
    enum RebalanceType {
        CurrentOpt(0), MaxExecutorOpt(1), TwoThreshold(2);
        private final int value;

        RebalanceType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static String getTypeSting(int value) {
            switch (value) {
                case 0:
                    return "CurrentOpt";
                case 1:
                    return "MaxExecutorOpt";
                case 2:
                    return "TwoThreshold";
                default:
                    return "unknown";
            }
        }
    }

    private long startTimeMillis;
    private long minExpectedIntervalMillis;
    private int rbTypeValue;

    @Override
    public void init(Map<String, Object> conf, StormTopology rawTopology) {
        startTimeMillis = System.currentTimeMillis();
        long calcIntervalSec = ConfigUtil.getInt(conf, OPTIMIZE_INTERVAL, 30);
        /** if OPTIMIZE_MIN_EXPECTED_REBALANCE_INTERVAL is not found in configuration files,
             * we use twice of OPTIMIZE_INTERVAL as default
         * here we -50 for synchronization purpose, this needs to be tested **/
        minExpectedIntervalMillis = ConfigUtil.getLong(conf, ResaConfig.OPTIMIZE_MIN_EXPECTED_REBALANCE_INTERVAL, calcIntervalSec * 2) * 1000 - 50;
        rbTypeValue = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_REBALANCE_TYPE, RebalanceType.CurrentOpt.getValue());

        //List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        //int port = Math.toIntExact((long) conf.get(Config.STORM_ZOOKEEPER_PORT));
        //client = DRSzkHandler.newClient(zkServer.get(0).toString(), port, 6000, 6000, 1000, 3);
        topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        testRatio = (double) conf.get("test.shedding.rate");
        LOG.info("SheddingBasicDecisionMaker.init(), stTime: " + startTimeMillis + ", minExpInteval: " + minExpectedIntervalMillis);
    }

    @Override
    public Map<String, Integer> make(ShedRateAndAllocResult newResult, Map<String, Integer> currAlloc) {
        long timeSpan = Math.max(0, System.currentTimeMillis() - startTimeMillis);
        if (newResult == null) {
            LOG.info("SheddingBasicDecisionMaker.make(), newResult == null");
            return null;
        }

        Map<String, Map<String,Double>> activeShedRate = newResult.getActiveShedRate();

        System.out.println("sheddingBasicDecisionMaker activeRate:"+activeShedRate);
        AllocResult newAllocResult = newResult.getAllocResult();
        if (timeSpan < minExpectedIntervalMillis) {
            /** if  timeSpan is not large enough, no rebalance will be triggered **/
            LOG.info("BasicDecisionMaker.make(), timeSpan (" + timeSpan + ") < minExpectedIntervalMillis (" + minExpectedIntervalMillis + ")");
            return null;
        }

        try {
            DRSzkHandler.sentActiveSheddingRate(activeShedRate.get("adjustedActiveShedRate"), topologyName, DRSzkHandler.lastDecision.DECISIONMAKE);
            //DRSzkHandler.sentActiveSheddingRate(testBuildShedRatio(), topologyName, DRSzkHandler.lastDecision.DECISIONMAKE);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (rbTypeValue == DefaultDecisionMaker.RebalanceType.MaxExecutorOpt.getValue()) {
            LOG.info("SheddingBasicDecisionMaker.make(), rbTypeValue == MaxExecutorOpt, rebalance is triggered with using maximum available resources");
            return newAllocResult.kMaxOptAllocation;
        } else {

            if (newAllocResult.status.equals(AllocResult.Status.OVERPROVISIONING)) {

                LOG.info("SheddingBasicDecisionMaker.make(), newAllocResult.status == OVERPROVISIONING, rebalance is triggered with removing existing resources");
                return newAllocResult.minReqOptAllocation;

            } else if (newAllocResult.status.equals(AllocResult.Status.SHORTAGE)) {

                LOG.info("SheddingBasicDecisionMaker.make(), newAllocResult.status == SHORTAGE, rebalance is triggered with adding new resources");
                return newAllocResult.minReqOptAllocation;

            } else if (newAllocResult.status.equals(AllocResult.Status.INFEASIBLE)) {

                LOG.info("SheddingBasicDecisionMaker.make(), newAllocResult.status == INFEASIBLE, rebalance is triggered with using maximum available resources");
                return newAllocResult.kMaxOptAllocation;

            } else if (newAllocResult.status.equals(AllocResult.Status.FEASIBLE)) {

                LOG.info("SheddingBasicDecisionMaker.make(), " +
                        "ewAllocResult.status == FEASIBLE, rebalance is triggered without adjusting current used resources, but re-allocation to optimal may be possible");
                ///return newAllocResult.currOptAllocation;
                return null;

            } else {
                throw new IllegalArgumentException("Illegal status: " + newAllocResult.status);
            }
        }
    }

    private Map<String,Double> testBuildShedRatio() {
        HashMap<String,Double> t = new HashMap();
        t.put("projection",0.0);
        t.put("objectSpout",Double.valueOf(String.format("%.2f", testRatio)));
        //testRatio += 0.1;
        t.put("updater",0.0);
        t.put("detector",0.0);
        System.out.println("cvteweipinhui: "+t);
        return t;
    }

}
