package resa.shedding.drswithshedding;

import org.apache.storm.generated.StormTopology;
import org.javatuples.Pair;
import resa.optimize.AggResult;
import resa.optimize.BoltAggResult;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.ResaUtils;



/**
 * Created by kailin on 30/3/17.
 * revert the real load
 */
public class RevertRealLoad {
    private static final Logger LOG = LoggerFactory.getLogger(RevertRealLoad.class);

    private StormTopology topology;
    private Integer historyLambdaSize;
    private Integer order;
    private ILearningSelectivity calcSelectivityFunction;
    private Map<String,Object> topologyTargets = new HashMap<>();
    private final Map<String,LinkedList<Pair<Double,Double>>> historyLambdaForSelectivity = new HashMap<>();
    private final ArrayList<RevertRealLoadData> revertRealLoadDatas = new ArrayList<>();


    public RevertRealLoad(Map<String, Object> conf, StormTopology stormTopology, Map<String, Object> targets) {
        historyLambdaSize = ConfigUtil.getInt(conf, ResaConfig.LAMBDA_FOR_SELECTIVITY_HISTORY_SIZE,10);
        topology = stormTopology;
        order = ConfigUtil.getInt(conf, ResaConfig.SELECTIVITY_FUNCTION_ORDER,1);
        calcSelectivityFunction = ResaUtils.newInstanceThrow(ConfigUtil.getString(conf, ResaConfig.SELECTIVITY_CALC_CLASS,
                PolynomialRegression.class.getName()),ILearningSelectivity.class);
        topologyTargets=targets;
        topologyTargets.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey())).forEach(e->{
            revertRealLoadDatas.add(new RevertRealLoadData(e.getKey()));
        });
    }


    /**
     * parse proportion,store load IN and OUT, learning selectivity function, set type, calculate real load.
     * */
    public void storeLoadInformation(Map<String, AggResult[]> comp2ExecutorResults) {
        parseAndStoreProportion(comp2ExecutorResults);
        storeLoadINandOUT(comp2ExecutorResults);
        if(parseAndStoreSelectivityFunction()) {
            setTypeOfComp();
            calcAndSetRealLoad(comp2ExecutorResults);
            clearSelectivityFuncAndLoad();
        }
     /*   comp2ExecutorResults.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey()))
                .forEach(g ->{
                    System.out.println(g.getKey());
                    AggResult[] tempAggResults = new AggResult[g.getValue().length];
                    for(AggResult aggResult : g.getValue()){
                        System.out.println(aggResult.getSendQueueResult().toString());
                        System.out.println(aggResult.getRecvQueueResult().toString());

                    }
                });*/
    }


    private void parseAndStoreProportion(Map<String, AggResult[]> comp2ExecutorResults) {
        comp2ExecutorResults.entrySet().stream().forEach(e->{
            Map<String,Long> exeAllEmitCountMap = new HashMap<>();
            for(int i=0;i<e.getValue().length;i++){
                System.out.println(e.getKey()+"i="+i);
                for(Map.Entry entry : e.getValue()[i].getemitCount().entrySet()){
                    long tempCount = (long) entry.getValue();
                    if(exeAllEmitCountMap.containsKey(entry.getKey())){
                        tempCount += exeAllEmitCountMap.get(entry.getKey());
                    }
                    exeAllEmitCountMap.put((String) entry.getKey(),tempCount);
                }
            }
            long denominator = 0;
            for(Long count : exeAllEmitCountMap.values()){
                denominator += count;
            }

            if(topologyTargets.containsKey(e.getKey())){//component key
                Map<String,ArrayList<String>> target = (Map<String, ArrayList<String>>) topologyTargets.get(e.getKey());
                for(Map.Entry exeAllEmitCountMapEntry:exeAllEmitCountMap.entrySet()){//stream ID
                    //System.out.println(target.keySet()+"rinimei"+exeAllEmitCountMapEntry.getKey());
                    if(target.containsKey(exeAllEmitCountMapEntry.getKey())){
                        ArrayList<String> minTarget = target.get(exeAllEmitCountMapEntry.getKey());//stream list
                        for(int i=0;i<minTarget.size();i++){
                            for(int j=0;j<revertRealLoadDatas.size();j++){
                                if(revertRealLoadDatas.get(j).getComponentId() == minTarget.get(i)){
                                    revertRealLoadDatas.get(j).addProportion(e.getKey(),((1.0*(long)exeAllEmitCountMapEntry.getValue())/denominator));
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    private void storeLoadINandOUT(Map<String, AggResult[]> comp2ExecutorResults) {
        comp2ExecutorResults.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey()))
                .forEach(e -> {
                    double tempLambdaIn =0.0;
                    double tempLambdaOut= 0.0;
                    for(int i=0;i<e.getValue().length;i++){
                        tempLambdaIn += e.getValue()[i].getArrivalRatePerSec() * (1.0-((BoltAggResult)e.getValue()[i]).getAvgSheddingRateHis());
                        tempLambdaOut += e.getValue()[i].getDepartureRatePerSec();
                    }
                    if(historyLambdaForSelectivity.containsKey(e.getKey())){
                        if(historyLambdaForSelectivity.get(e.getKey()).size() == historyLambdaSize){
                            LOG.info("update history lambda list !");
                            historyLambdaForSelectivity.get(e.getKey()).poll();
                        }else{
                            if(historyLambdaForSelectivity.get(e.getKey()).size() > historyLambdaSize){
                                int i = historyLambdaForSelectivity.get(e.getKey()).size() - historyLambdaSize;
                                while(i>=0){
                                    historyLambdaForSelectivity.get(e.getKey()).poll();
                                    i--;
                                }
                            }
                        }
                        historyLambdaForSelectivity.get(e.getKey()).offer(new Pair<>(tempLambdaIn,tempLambdaOut));
                    }else{
                        LinkedList<Pair<Double,Double>> lambdaPairList = new LinkedList<>();
                        lambdaPairList.offer(new Pair<>(tempLambdaIn,tempLambdaOut));
                        historyLambdaForSelectivity.put(e.getKey(),lambdaPairList);
                    }
                });
    }

    private boolean parseAndStoreSelectivityFunction() {
        boolean flag = true;
        for(Map.Entry entry : historyLambdaForSelectivity.entrySet()){
            List list = (List) entry.getValue();
            if(list.size() < historyLambdaSize){
                flag = false;
                LOG.info("have a component "+entry.getKey()+" size: "+list.size()+" < "+historyLambdaSize);
                break;
            }
        }
        if(flag){
            LOG.info("begin to calculate selectivityFunction!");
            for(Map.Entry entry : historyLambdaForSelectivity.entrySet()){
                double[] oneCompSelectivityCoeff = calcSelectivityFunction.Fit(entry.getValue(),order);
                for(int j=0;j<oneCompSelectivityCoeff.length;j++)
                    System.out.println(oneCompSelectivityCoeff[j]+"j==="+j);
                for(int j=0;j<revertRealLoadDatas.size();j++){
                    if(revertRealLoadDatas.get(j).getComponentId().hashCode() == entry.getKey().hashCode()){
                        revertRealLoadDatas.get(j).addCoeff(oneCompSelectivityCoeff);
                        break;
                    }
                }
            }
        }
        return flag;

    }

    private void setTypeOfComp() {
        for(int j=0;j<revertRealLoadDatas.size();j++){
            int type =0;
            Map<String, Double> tempProportion = revertRealLoadDatas.get(j).getProportion();
            for(Map.Entry entry: tempProportion.entrySet()){
                if(topology.get_spouts().containsKey(entry.getKey())){
                    type++;
                }
            }
            if(type == topology.get_spouts_size()){
                type=1;
            }
            else if(type != 0){
                type =2;
            }else{
                type=3;
            }
            revertRealLoadDatas.get(j).setType(type);
        }
    }

    private void calcAndSetRealLoad(Map<String, AggResult[]> comp2ExecutorResults) {
        Map<String,Double> sourceLoads = new HashMap<>();
        comp2ExecutorResults.entrySet().stream().filter(e -> topology.get_spouts().containsKey(e.getKey()))
                .forEach(e -> {
                    double lambdaSource = 0.0;
                    for(int i=0;i<e.getValue().length;i++){
                        System.out.println(e.getValue()[i].getDepartureRatePerSec());
                        lambdaSource += e.getValue()[i].getDepartureRatePerSec();
                    }
                    sourceLoads.put(e.getKey(),lambdaSource);
                    System.out.println(sourceLoads.toString()+"yoho!");
        });
        calcType1BoltRealLoad(sourceLoads);
        calcType2BoltRealLoad(sourceLoads);
        calcType3BoltRealLoad(sourceLoads);
        for(int i=0;i<revertRealLoadDatas.size();i++){
            if(revertRealLoadDatas.get(i).getRealLoadOUT() <0){
                System.out.println("begin rebuild load"+revertRealLoadDatas.get(i).getRealLoadOUT());
                calcType2BoltRealLoad(sourceLoads);
                calcType3BoltRealLoad(sourceLoads);
                System.out.println("end rebuild load!!!!!!!!!!"+revertRealLoadDatas.get(i).getRealLoadOUT());
            }
        }
        for(int i=0;i<revertRealLoadDatas.size();i++){
            System.out.println(revertRealLoadDatas.get(i).toString());
        }

        comp2ExecutorResults.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey()))
                .forEach(g ->{
                    for(AggResult aggResult : g.getValue()){
                        System.out.println(aggResult.getSendQueueResult().toString());
                        System.out.println(aggResult.getRecvQueueResult().toString());

                    }
                });

    }

    private void calcType3BoltRealLoad(Map<String, Double> sourceLoads) {
        revertRealLoadDatas.stream().filter(e->e.getType() == 3).forEach(e ->{
            boolean flag = true;
            ArrayList<Double> coeff = e.getSelectivityFunction();
            double readLoadOUT = 0.0;
            double appLoadIn = 0.0;
            for(Map.Entry entry : e.getProportion().entrySet()){
                for(int i=0;i<revertRealLoadDatas.size();i++){
                    if(revertRealLoadDatas.get(i).getComponentId().hashCode() == entry.getKey().hashCode()){
                        if(revertRealLoadDatas.get(i).getRealLoadOUT() >= 0) {
                            appLoadIn += (revertRealLoadDatas.get(i).getRealLoadOUT() * (double) entry.getValue());
                        }else{
                            LOG.info("can not calculate this component(type3) load! will be recalculated later");
                            flag = false;
                        }
                        break;
                    }
                }
                if(!flag){break;}
            }
            System.out.println(appLoadIn+"calcType3BoltRealLoad:flag"+flag);
            if(flag) {
                for (int j = 0; j < coeff.size(); j++) {
                    readLoadOUT += (Math.pow(appLoadIn, j) * coeff.get(j));
                }
                e.setRealLoadOUT(readLoadOUT);
                e.setRealLoadIN(appLoadIn);
            }
            System.out.println(e.getComponentId()+"calcType3BoltRealLoad:"+"getRealLoadOUT:"+readLoadOUT+": appLOad"+appLoadIn);
        });
    }

    private void calcType2BoltRealLoad(Map<String, Double> sourceLoads) {
        revertRealLoadDatas.stream().filter(e->e.getType() == 2).forEach(e ->{
            boolean flag = true;
            ArrayList<Double> coeff = e.getSelectivityFunction();
            double readLoadOUT = 0.0;
            double appLoadIn = 0.0;
            for(Map.Entry entry : e.getProportion().entrySet()){
                if(topology.get_bolts().containsKey(entry.getKey())){
                    for(int i=0;i<revertRealLoadDatas.size();i++){
                        if(revertRealLoadDatas.get(i).getComponentId().hashCode() == entry.getKey().hashCode()){
                            if(revertRealLoadDatas.get(i).getRealLoadOUT() >= 0) {
                                appLoadIn += (revertRealLoadDatas.get(i).getRealLoadOUT() * (double) entry.getValue());
                            }else{
                                LOG.info("can not calculate this component(type2) load! will be recalculated later");
                                flag = false;
                            }
                            break;
                        }
                    }
                }else{
                    appLoadIn += (sourceLoads.get(entry.getKey()) * (double)entry.getValue());
                }
                if(!flag){break;}
            }
            System.out.println(appLoadIn+"calcType2BoltRealLoad:flag"+flag);
            if(flag) {
                for (int j = 0; j < coeff.size(); j++) {
                    readLoadOUT += (Math.pow(appLoadIn, j) * coeff.get(j));
                }
                e.setRealLoadOUT(readLoadOUT);
                e.setRealLoadIN(appLoadIn);
            }
            System.out.println(e.getComponentId()+"calcType2BoltRealLoad:"+"getRealLoadOUT:"+readLoadOUT+": appLOad"+appLoadIn);
        });
    }

    private void calcType1BoltRealLoad(Map<String, Double> sourceLoads) {
        revertRealLoadDatas.stream().filter(e->e.getType() == 1).forEach(e ->{
            ArrayList<Double> coeff = e.getSelectivityFunction();
            double readLoadOUT = 0.0;
            double appLoadIn = 0.0;
            for(Map.Entry entry : e.getProportion().entrySet()){
                appLoadIn += (sourceLoads.get(entry.getKey()) * (double)entry.getValue());
            }
            for(int j=0; j<coeff.size();j++){
                readLoadOUT += (Math.pow(appLoadIn,j) * coeff.get(j));
            }
            e.setRealLoadOUT(readLoadOUT);
            e.setRealLoadIN(appLoadIn);
            System.out.println(e.getComponentId()+"calcType1BoltRealLoad:"+"getRealLoadOUT:"+readLoadOUT+": appLOad"+appLoadIn);
        });
    }

    private void clearSelectivityFuncAndLoad() {
        revertRealLoadDatas.forEach(e -> e.clear());
    }

}
