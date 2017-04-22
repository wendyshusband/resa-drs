package resa.shedding.drswithshedding;

import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.optimize.*;
import java.util.*;

/**
 * Created by kailin on 12/4/17.
 */
public class SheddingLoadRevert {
    private static final Logger LOG = LoggerFactory.getLogger(SheddingLoadRevert.class);

    private Map<String,RevertRealLoadData> revertRealLoadDatas = new HashMap<>();
    private Map<String,Object> topologyTargets = new HashMap<>();
    private StormTopology topology;
    private Map<String, double[]> selectivityFunctions = new HashMap<>();
    private SourceNode sourceNode;
    private Map<String, ServiceNode> serviceNodeMap;
    private List<String> topoSortResult = new ArrayList<>();

    public SheddingLoadRevert(SourceNode spInfo, Map<String, ServiceNode> queueingNetwork, StormTopology stormTopology,
                              Map<String, Object> targets, Map<String, double[]> selectivityFunctions) {
        topology = stormTopology;
        topologyTargets=targets;
        topologyTargets.entrySet().stream().filter(e -> topology.get_bolts().containsKey(e.getKey())).forEach(e->{
            revertRealLoadDatas.put(e.getKey(),new RevertRealLoadData(e.getKey()));
        });
        this.selectivityFunctions = selectivityFunctions;
        sourceNode = spInfo;
        serviceNodeMap = queueingNetwork;
    }

    public void revertLoad() {
        calcProportion();
        calcAndSetRealLoad();
    }

    private void calcProportion() {
        LOG.info("calculate proportion !");
        calcSourceNodeProportion();
        calcServiceNodeProportion();
    }

    private void calcServiceNodeProportion() {
        for(Map.Entry serviceNode : serviceNodeMap.entrySet()){
            Map<String,Long> allEmitCountMap = ((ServiceNode)serviceNode.getValue()).getEmitCount();
            long denominator = allEmitCountMap.values().stream().mapToLong(Number::longValue).sum();
            LOG.info("serviceNODE : "+serviceNode.getKey()+" whole emit tuple number ="+ denominator);
            Map<String,ArrayList<String>> stream2CompLists =
                    (Map<String, ArrayList<String>>) topologyTargets.get(serviceNode.getKey());
            if(!stream2CompLists.isEmpty()) {
                for(Map.Entry stream2CompList : stream2CompLists.entrySet()){
                    ArrayList<String> compList = (ArrayList<String>) stream2CompList.getValue();
                    if(allEmitCountMap.containsKey(stream2CompList.getKey())) {
                        for (int i = 0; i < compList.size(); i++) {
                            revertRealLoadDatas.get(compList.get(i)).addProportion((String) serviceNode.getKey(),
                                    (1.0 * allEmitCountMap.get(stream2CompList.getKey())) / denominator);
                        }
                    }else{
                        for (int i = 0; i < compList.size(); i++) {
                            revertRealLoadDatas.get(compList.get(i)).addProportion((String) serviceNode.getKey(),
                                    0.0);
                        }
                    }
                }
            }
        }
    }

    private void calcSourceNodeProportion() {
        Map<String,Long> emitCountMap = sourceNode.getEmitCount();
        long denominator = 0;
        for(Long count : emitCountMap.values()){
            denominator += count;
        }
        LOG.info("sourceNode : "+sourceNode.getComponentID()+" whole emit tuple number ="+ denominator);
        Map<String,ArrayList<String>> stream2CompLists =
                (Map<String, ArrayList<String>>) topologyTargets.get(sourceNode.getComponentID());
        if(!stream2CompLists.isEmpty()) {
            for(Map.Entry stream2CompList : stream2CompLists.entrySet()){
                ArrayList<String> compList = (ArrayList<String>) stream2CompList.getValue();
                if(emitCountMap.containsKey(stream2CompList.getKey())) {
                    for (int i = 0; i < compList.size(); i++) {
                        revertRealLoadDatas.get(compList.get(i)).addProportion(sourceNode.getComponentID(),
                                (1.0 * emitCountMap.get(stream2CompList.getKey())) / denominator);
                    }
                }else{
                    for (int i = 0; i < compList.size(); i++) {
                        revertRealLoadDatas.get(compList.get(i)).addProportion(sourceNode.getComponentID(),
                                0.0);
                    }
                }
            }
        }
    }

    private void calcAndSetRealLoad(){
        LOG.info("calculate and set real load !");
        double sourceLoad = sourceNode.getTupleEmitRateOnSQ();
        System.out.println(sourceLoad+ "  result.isEmpty()="+topoSortResult.isEmpty());
        if(topoSortResult.isEmpty()) {
            SheddingLoadRevert.TopoSort topoSort = new SheddingLoadRevert.TopoSort();
            topoSort.createGraph(topology, topologyTargets, revertRealLoadDatas);
            topoSort.kahnProcess();
            topoSortResult = topoSort.getResult();
            //topoSort.outputResult();
        }
        for(int i=0; i<topoSortResult.size(); i++){
            double readLoadOUT = 0.0;
            double appLoadIn = 0.0;
            System.out.println(topoSortResult.get(i)+":*****:"+revertRealLoadDatas.get(topoSortResult.get(i)).getProportion());
            for(Map.Entry entry : revertRealLoadDatas.get(topoSortResult.get(i)).getProportion().entrySet()){
                System.out.println(entry.getKey()+"load shedding :: apploadIN and loadOUT!!!!!!!"+entry.getValue());
                if(topology.get_bolts().containsKey(entry.getKey())){
                    System.out.println("OUT: "+revertRealLoadDatas.get(entry.getKey()).getRealLoadOUT());
                    System.out.println("PROPORTION: "+entry.getValue());
                    appLoadIn += (revertRealLoadDatas.get(entry.getKey()).getRealLoadOUT() * (double) entry.getValue());
                }else{
                    System.out.println(entry.getKey()+"#~~~ sourceLoad ~~~#"+sourceLoad);
                    System.out.println("PROPORTION: "+entry.getValue());
                    appLoadIn += (sourceLoad * (double)entry.getValue());
                }
            }
            double[] coeff =selectivityFunctions.get(topoSortResult.get(i));
            for(int j=0; j<coeff.length;j++){
                readLoadOUT += (Math.pow(appLoadIn,j) * coeff[j]);
            }
            System.out.println(readLoadOUT+"----OUT------------------------IN--------"+appLoadIn);
            revertRealLoadDatas.get(topoSortResult.get(i)).setRealLoadOUT(readLoadOUT);
            revertRealLoadDatas.get(topoSortResult.get(i)).setRealLoadIN(appLoadIn);
        }
        revertRealLoadDatas.entrySet().stream().forEach(e->{
            System.out.println("before: "+serviceNodeMap.get(e.getKey()).toString());
            serviceNodeMap.get(e.getKey()).revertLambda(e.getValue().getRealLoadIN());
            System.out.println("after: "+serviceNodeMap.get(e.getKey()).toString());
        });


    }

    private static class TopoSort{
        private HashMap<String,Integer> vertexMap = new HashMap<>();
        private HashMap<String,ArrayList<String>> adjaNode = new HashMap<>();
        private Queue<String> setOfZeroIndegree = new LinkedList<>();
        private List<String> result = new ArrayList<>();
        private void createGraph(StormTopology topology,
                                 Map<String,Object> topologyTargets,
                                 Map<String,RevertRealLoadData> revertRealLoadDatas){
            revertRealLoadDatas.entrySet().stream().forEach(e->{
                int pathIn = 0;
                for(String compId : e.getValue().getProportion().keySet()) {
                    if(topology.get_bolts().containsKey(compId)){//only add pre bolt
                        pathIn++;
                    }
                }
                vertexMap.put(e.getKey(),pathIn);
            });

            for(String key : vertexMap.keySet()){
                if(topologyTargets.containsKey(key)){
                    adjaNode.put(key,new ArrayList<>()); // init
                    Map<String,ArrayList<String>> stream2CompList =
                            (Map<String, ArrayList<String>>) topologyTargets.get(key);
                    if(!stream2CompList.isEmpty()) {
                        for(ArrayList<String> successor : stream2CompList.values()){
                            for(String comp : successor){
                                if(!adjaNode.get(key).contains(comp)){
                                    adjaNode.get(key).add(comp);
                                }
                            }
                        }
                    }
                }
            }
        }

        private void kahnProcess(){
            for(Map.Entry entry : vertexMap.entrySet()){
                if(0 == (int)entry.getValue()){
                    setOfZeroIndegree.add((String) entry.getKey());
                }
            }
            int tempPathIN;
            while(!setOfZeroIndegree.isEmpty()){
                String node = setOfZeroIndegree.poll();
                result.add(node);
                if(adjaNode.keySet().isEmpty()){
                    return;
                }
                for(String successor : adjaNode.get(node)){
                    tempPathIN =  vertexMap.get(successor) - 1;
                    if(tempPathIN == 0){
                        setOfZeroIndegree.add(successor);
                    }
                    vertexMap.put(successor,tempPathIN);
                }
                vertexMap.remove(node);
                adjaNode.remove(node);
            }

            if(!vertexMap.isEmpty()){
                throw new IllegalArgumentException("Has cycle !");
            }
        }

        public List<String> getResult() {
            return result;
        }

        public void outputResult(){
            System.out.println("_________________result_______________");
            for(int i=0 ;i<result.size(); i++)
                System.out.println(result.get(i));
            System.out.println("_______________________________________");
        }
    }
}