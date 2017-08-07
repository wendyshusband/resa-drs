package resa.shedding.basicServices;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.optimize.AggResult;
import resa.optimize.SpoutAggResult;
import resa.shedding.tools.DRSzkHandler;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static resa.shedding.basicServices.SpoutActiveSheddingRateTrimer.LatencyStatus.*;

/**
 * Created by 44931 on 2017/7/29.
 */
public class SpoutActiveSheddingRateTrimer extends ActiveSheddingRateTrimer {

    public static final Logger LOG = LoggerFactory.getLogger(SpoutActiveSheddingRateTrimer.class);
    //public static double anInt = 0.1415926;
    enum LatencyStatus {
        HIGH(1), PRECISELY(0), LOW(-1);
        private final int value;

        LatencyStatus(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static String getStatus(int value){
            switch (value){
                case 0: return "PRECISELY";
                case 1: return "HIGH";
                case -1: return "LOW";
                default: return "unknown";
            }
        }
    }

    @Override
    public void trim(Map<String, AggResult[]> executorAggResults) {

        double completeTimeMilliSecUpper = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_QOS_UPPER_MS, 2000.0);
        int latencyTolerant = ConfigUtil.getInt(conf, ResaConfig.LATENCY_DIFFERENCE_THRESHOLD_UNDER_ACTIVE_SHEDDING_STATUS, 150);
        double activeShedRateIncrement =  ConfigUtil.getDouble(conf, ResaConfig.ACTIVE_SHEDDINGRATE_TRIM_INCREMENT, 0.05);

        Map<String, LatencyStatus> spoutLatencyDifference = new HashMap<>();
        ArrayList<String> bolts = new ArrayList();

        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .forEach(e -> bolts.add(e.getKey()));

        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .forEach(e -> {
                    AggResult CompAggResult = new SpoutAggResult();
                    AggResult.getVerticalCombinedResult(CompAggResult, Arrays.asList(e.getValue()));
                    double tempResult = Double.valueOf(String.format("%.1f",((SpoutAggResult) CompAggResult).getAvgTupleCompleteLatency()))
                            - completeTimeMilliSecUpper;
                    System.out.println("nowsheddingdifference: "+tempResult);
                    LatencyStatus difference = tempResult > latencyTolerant
                            ? HIGH : ((tempResult < (-latencyTolerant)) ? LOW : PRECISELY);
                    spoutLatencyDifference.put(e.getKey(), difference);
                });
        try {
            if (DRSzkHandler.clientIsStart() && null != client.checkExists().forPath("/drs/"+topologyName)) {
                byte[] oldActiveShedRateMap = client.getData().forPath("/drs/" + topologyName);
                Map<String, Double> newActiveShedRateMap = new HashMap<>();
                //anInt += 1.0;
                for (String bolt : bolts) {
                    newActiveShedRateMap.put(bolt, DRSzkHandler.parseActiveShedRateMap(oldActiveShedRateMap, bolt));
                    //newActiveShedRateMap.put(bolt, anInt);
                }
                System.out.println(newActiveShedRateMap.toString() + "paris1");
                boolean changeFlag = false;
                for (Map.Entry entry : spoutLatencyDifference.entrySet()) {
                    double originRate = DRSzkHandler.parseActiveShedRateMap(oldActiveShedRateMap, (String) entry.getKey());
                    double newRate = originRate;
                    System.out.println("old rate: " + newRate);
                    if (newRate >= activeShedRateIncrement) {
                        LatencyStatus latencyStatus = (LatencyStatus) entry.getValue();
                        if (latencyStatus == LatencyStatus.LOW) {
                            newRate -= activeShedRateIncrement;
                        } else if (latencyStatus == LatencyStatus.HIGH) {
                            newRate += activeShedRateIncrement;
                        }
                    }
                    newRate = Double.valueOf(String.format("%.2f", newRate));
                    if (newRate != originRate) {
                        changeFlag = true;
                        newActiveShedRateMap.put((String) entry.getKey(), newRate);
                        System.out.println("new rate: " + newRate);
                    }
                }
                System.out.println(newActiveShedRateMap.toString() + "paris2");
                if (changeFlag) {
                    DRSzkHandler.sentActiveSheddingRate(newActiveShedRateMap, topologyName, DRSzkHandler.lastDecision.TRIM);
                    LOG.info("finish trim active shedding rate!");
                } else {
                    LOG.info("no change of active shedding rate! no need trim");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
