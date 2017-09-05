//package resa.shedding.example;
//
//import org.apache.storm.generated.StormTopology;
//import org.javatuples.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import resa.optimize.AggResult;
//import resa.shedding.basicServices.api.LearningModel;
//import resa.shedding.basicServices.RevertRealLoadData;
//import resa.util.ConfigUtil;
//import resa.util.ResaConfig;
//import resa.util.ResaUtils;
//
//import java.util.*;
//
//
//
///**
// * Created by kailin on 30/3/17.
// * revert the real load
// */
//public class RevertRealLoad {
//    private static final Logger LOG = LoggerFactory.getLogger(RevertRealLoad.class);
//
//    private StormTopology topology;
//    private Integer historyLambdaSize;
//    private Integer order;
//    private LearningModel calcSelectivityFunction;
//    private Map<String,Object> topologyTargets = new HashMap<>();
//    private final Map<String,LinkedList<Pair<Double,Double>>> historyLambdaForSelectivity = new HashMap<>();
//    private final Map<String,RevertRealLoadData> revertRealLoadDatas = new HashMap<>();
//    private List<String> result = new ArrayList<>();
//
//
//    public RevertRealLoad(Map<String, Object> conf, StormTopology stormTopology, Map<String, Object> targets) {
//        historyLambdaSize = ConfigUtil.getInt(conf, ResaConfig.LAMBDA_FOR_SELECTIVITY_HISTORY_SIZE,10);
//        topology = stormTopology;
//        order = ConfigUtil.getInt(conf, ResaConfig.SELECTIVITY_FUNCTION_ORDER,1);
//        calcSelectivityFunction = ResaUtils.newInstanceThrow(ConfigUtil.getString(conf, ResaConfig.SELECTIVITY_CALC_CLASS,
//                PolynomialRegression.class.getName()),LearningModel.class);
//        topologyTargets=targets;
//        topologyTargets.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey())).forEach(e->{
//            revertRealLoadDatas.put(e.getKey(),new RevertRealLoadData(e.getKey()));
//        });
//    }
//
//    public StormTopology getTopology() {
//        return topology;
//    }
//
//    public Integer getOrder() {
//        return order;
//    }
//
//    public Map<String, Object> getTopologyTargets() {
//        return topologyTargets;
//    }
//
//
//
//    /**
//     * parse proportion,store load IN and OUT, learning selectivity function, set type, calculate real load.
//     * */
//    public Map<String, RevertRealLoadData> storeLoadInformation(Map<String, AggResult[]> comp2ExecutorResults) {
//        clearSelectivityFuncAndLoad();
//        parseAndStoreProportion(comp2ExecutorResults);
//        storeLoadINandOUT(comp2ExecutorResults);
//        if(parseAndStoreSelectivityFunction()) {
//            calcAndSetRealLoad(comp2ExecutorResults);
//            //clearSelectivityFuncAndLoad();
//            //clearProportion();
//        }
//        return revertRealLoadDatas;
//    }
//
//    /*private void updateComp2ExecutorResults(Map<String, AggResult[]> comp2ExecutorResults) {
//        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~A~~~~~~~~~~~~~~~");
//        comp2ExecutorResults.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey()))
//                .forEach(g ->{
//                    System.out.println(g.getKey());
//                    AggResult[] tempAggResults = new AggResult[g.getValue().length];
//                    for(AggResult aggResult : g.getValue()){
//                        System.out.println("send!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                        System.out.println(aggResult.getSendQueueResult().toString());
//                        System.out.println(((CntMeanVar)aggResult.getSendQueueResult().getQueueArrivalRate()).toCMVString());
//                        System.out.println("receive!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                        System.out.println(aggResult.getRecvQueueResult().toString());
//                        System.out.println(((CntMeanVar)aggResult.getRecvQueueResult().getQueueArrivalRate()).toCMVString());
//                    }
//                });
//    }*/
//
//
//    private void parseAndStoreProportion(Map<String, AggResult[]> comp2ExecutorResults) {
//        comp2ExecutorResults.entrySet().stream().forEach(e->{
//            Map<String,Long> exeAllEmitCountMap = new HashMap<>();
//            for(int i=0;i<e.getValue().length;i++){//component
//                for(Map.Entry entry : e.getValue()[i].getemitCount().entrySet()){//executor
//                    long tempCount = (long) entry.getValue();
//                    if(exeAllEmitCountMap.containsKey(entry.getKey())){
//                        tempCount += exeAllEmitCountMap.get(entry.getKey());
//                    }
//                    exeAllEmitCountMap.put((String) entry.getKey(),tempCount);
//                }
//            }
//            long denominator = 0;
//            for(Long count : exeAllEmitCountMap.values()){
//                denominator += count;
//            }
//
//            LOG.info(e.getKey()+" whole emit tuple number ="+ denominator);
//            if(topologyTargets.containsKey(e.getKey())){//component key
//                Map<String,ArrayList<String>> stream2CompList = (Map<String, ArrayList<String>>) topologyTargets.get(e.getKey());
//                //System.out.println(stream2CompList.isEmpty()+"meiyisi"+e.getKey()+"youyisima"+stream2CompList);
//                if(!stream2CompList.isEmpty()) {
//                    for (Map.Entry exeAllEmitCountMapEntry : exeAllEmitCountMap.entrySet()) {//stream ID
//                        if (stream2CompList.containsKey(exeAllEmitCountMapEntry.getKey())) {
//                            ArrayList<String> compList = stream2CompList.get(exeAllEmitCountMapEntry.getKey());//comp list
//                            for (int i = 0; i < compList.size(); i++) {
//                                if(revertRealLoadDatas.containsKey(compList.get(i))){
//                                    //System.out.println(compList.get(i)+"suanshenmenanren"+((1.0 * (long) exeAllEmitCountMapEntry.getValue()) / denominator));
//                                    revertRealLoadDatas.get(compList.get(i)).addProportion(e.getKey(), ((1.0 * (long) exeAllEmitCountMapEntry.getValue()) / denominator));
//                                }else {
//                                    LOG.error("revertRealLoadDatas have no this component" + compList.get(i));
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        });
//    }
//
//    private void storeLoadINandOUT(Map<String, AggResult[]> comp2ExecutorResults) {
//        comp2ExecutorResults.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey()))
//                .forEach(e -> {
//                    double tempLambdaIn =0.0;
//                    double tempLambdaOut= 0.0;
//                    for(int i=0;i<e.getValue().length;i++){
//                        System.out.println(e.getValue()[i].getArrivalRatePerSec());
//                        //System.out.println(((BoltAggResult)e.getValue()[i]).getAvgSheddingRateHis());
//                        System.out.println(e.getValue()[i].getDepartureRatePerSec());
//                        tempLambdaIn +=0;// e.getValue()[i].getArrivalRatePerSec() * (1.0-((BoltAggResult)e.getValue()[i]).getAvgSheddingRateHis());
//                        tempLambdaOut += e.getValue()[i].getDepartureRatePerSec();
//                    }
//                    System.out.println(e.getKey()+"()()()"+tempLambdaIn+"()()()"+tempLambdaOut);
//                    if(historyLambdaForSelectivity.containsKey(e.getKey())){
//                        if(historyLambdaForSelectivity.get(e.getKey()).size() == historyLambdaSize){
//                            LOG.info("update history lambda list !");
//                            historyLambdaForSelectivity.get(e.getKey()).poll();
//                        }else{
//                            if(historyLambdaForSelectivity.get(e.getKey()).size() > historyLambdaSize){
//                                int i = historyLambdaForSelectivity.get(e.getKey()).size() - historyLambdaSize;
//                                while(i>=0){
//                                    historyLambdaForSelectivity.get(e.getKey()).poll();
//                                    i--;
//                                }
//                            }
//                        }
//                        historyLambdaForSelectivity.get(e.getKey()).offer(new Pair<>(tempLambdaIn,tempLambdaOut));
//                    }else{
//                        LinkedList<Pair<Double,Double>> lambdaPairList = new LinkedList<>();
//                        lambdaPairList.offer(new Pair<>(tempLambdaIn,tempLambdaOut));
//                        historyLambdaForSelectivity.put(e.getKey(),lambdaPairList);
//                    }
//                });
//    }
//
//    private boolean parseAndStoreSelectivityFunction() {
//        boolean flag = true;
//        for(Map.Entry entry : historyLambdaForSelectivity.entrySet()){
//            List list = (List) entry.getValue();
//            if(list.size() < historyLambdaSize){
//                flag = false;
//                LOG.info("have a component "+entry.getKey()+" size: "+list.size()+" < "+historyLambdaSize);
//                break;
//            }
//        }
//        if(flag){
//            LOG.info("Begin to calculate selectivityFunction!");
//            for(Map.Entry entry : historyLambdaForSelectivity.entrySet()){
//                System.out.println(entry.getKey());
//
//                double[] oneCompSelectivityCoeff = calcSelectivityFunction.Fit(entry.getValue(),order);
//                if(revertRealLoadDatas.containsKey(entry.getKey())){
//                    revertRealLoadDatas.get(entry.getKey()).addCoeff(oneCompSelectivityCoeff);
//                }else{
//                    LOG.error("revertRealLoadDatas have no this component"+entry.getKey());
//                }
//            }
//        }
//        return flag;
//
//    }
//
//    private void calcAndSetRealLoad(Map<String, AggResult[]> comp2ExecutorResults) {
//        LOG.info("calculate and set real load !");
//        Map<String, Double> sourceLoads = new HashMap<>();
//        comp2ExecutorResults.entrySet().stream().filter(e -> topology.get_spouts().containsKey(e.getKey()))
//                .forEach(e -> {
//                    double lambdaSource = 0.0;
//                    lambdaSource = e.getValue()[0].getDepartureRatePerSec()*e.getValue().length;
//                    System.out.println(e.getValue()[0].getDepartureRatePerSec()+"abcdefg"+e.getValue().length);
//                    sourceLoads.put(e.getKey(), lambdaSource);
//                });
//        System.out.println(sourceLoads+ "  result.isEmpty()="+result.isEmpty());
//        if(result.isEmpty()) {
//            TopoSort topoSort = new TopoSort();
//            topoSort.createGraph(topology, topologyTargets, revertRealLoadDatas);
//            topoSort.kahnProcess();
//            result = topoSort.getResult();
//        }
//        for(int i=0; i<result.size(); i++){
//            ArrayList<Double> coeff = revertRealLoadDatas.get(result.get(i)).getSelectivityFunction();
//            double readLoadOUT = 0.0;
//            double appLoadIn = 0.0;
//            System.out.println(result.get(i)+":**:"+revertRealLoadDatas.get(result.get(i)).getProportion());
//            for(Map.Entry entry : revertRealLoadDatas.get(result.get(i)).getProportion().entrySet()){
//                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
//                if(topology.get_bolts().containsKey(entry.getKey())){
//                    System.out.println(revertRealLoadDatas.get(entry.getKey()).getRealLoadOUT());
//                    System.out.println(entry.getValue());
//                    appLoadIn += (revertRealLoadDatas.get(entry.getKey()).getRealLoadOUT() * (double) entry.getValue());
//                }else{
//                    System.out.println(sourceLoads.get(entry.getKey()));
//                    System.out.println(entry.getValue());
//                    appLoadIn += (sourceLoads.get(entry.getKey()) * (double)entry.getValue());
//                }
//            }
//            for(int j=0; j<coeff.size();j++){
//                readLoadOUT += (Math.pow(appLoadIn,j) * coeff.get(j));
//            }
//            System.out.println(readLoadOUT+"zuochuxuanze"+appLoadIn);
//            revertRealLoadDatas.get(result.get(i)).setRealLoadOUT(readLoadOUT);
//            revertRealLoadDatas.get(result.get(i)).setRealLoadIN(appLoadIn);
//        }
//        System.out.println("*******************************result******************************");
//        for(Map.Entry entry : revertRealLoadDatas.entrySet()){
//            System.out.println(entry.getKey()+"~~~~"+entry.getValue());
//        }
//    }
//
//    private void clearSelectivityFuncAndLoad() {
//        revertRealLoadDatas.forEach((key,value)-> value.clear());
//    }
//
//    private void clearProportion() {
//        revertRealLoadDatas.forEach((key,value)-> value.clearProportion());
//    }
//    private static class TopoSort{
//        private HashMap<String,Integer> vertexMap = new HashMap<>();
//        private HashMap<String,ArrayList<String>> adjaNode = new HashMap<>();
//        private Queue<String> setOfZeroIndegree = new LinkedList<>();
//        private List<String> result = new ArrayList<>();
//        private void createGraph(StormTopology topology,
//                                 Map<String,Object> topologyTargets,
//                                 Map<String,RevertRealLoadData> revertRealLoadDatas){
//            revertRealLoadDatas.entrySet().stream().forEach(e->{
//                int pathIn = 0;
//                for(String compId : e.getValue().getProportion().keySet()) {
//                    if(topology.get_bolts().containsKey(compId)){//only add pre bolt
//                        pathIn++;
//                    }
//                }
//                vertexMap.put(e.getKey(),pathIn);
//            });
//
//            for(String key : vertexMap.keySet()){
//                if(topologyTargets.containsKey(key)){
//                    adjaNode.put(key,new ArrayList<>()); // init
//                    Map<String,ArrayList<String>> stream2CompList =
//                            (Map<String, ArrayList<String>>) topologyTargets.get(key);
//                    if(!stream2CompList.isEmpty()) {
//                        for(ArrayList<String> successor : stream2CompList.values()){
//                            for(String comp : successor){
//                                if(!adjaNode.get(key).contains(comp)){
//                                    adjaNode.get(key).add(comp);
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        private void kahnProcess(){
//            for(Map.Entry entry : vertexMap.entrySet()){
//                if(0 == (int)entry.getValue()){
//                    setOfZeroIndegree.add((String) entry.getKey());
//                }
//            }
//            int tempPathIN;
//            while(!setOfZeroIndegree.isEmpty()){
//                String node = setOfZeroIndegree.poll();
//                result.add(node);
//                if(adjaNode.keySet().isEmpty()){
//                    return;
//                }
//                for(String successor : adjaNode.get(node)){
//                    tempPathIN =  vertexMap.get(successor) - 1;
//                    if(tempPathIN == 0){
//                        setOfZeroIndegree.add(successor);
//                    }
//                    vertexMap.put(successor,tempPathIN);
//                }
//                vertexMap.remove(node);
//                adjaNode.remove(node);
//            }
//
//            if(!vertexMap.isEmpty()){
//                throw new IllegalArgumentException("Has cycle !");
//            }
//        }
//
//        public List<String> getResult() {
//            return result;
//        }
//    }
//
//
//
//}
