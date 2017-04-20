package resa.optimize;

import clojure.lang.Numbers;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-5-6.
 */
public class BoltAggResult extends AggResult {
    //private Map<String, CntMeanVar> sheddingProcess = new HashMap<>();//tkl
    private Map<String, CntMeanVar> tupleProcess = new HashMap<>();

    protected Map<String, Long> sheddingCountMap = new HashMap<>();//tkl


    public Map<String, CntMeanVar> getTupleProcess() {
        return tupleProcess;
    }

    public CntMeanVar getCombinedProcessedResult() {
        CntMeanVar retVal = new CntMeanVar();
        tupleProcess.values().stream().forEach(retVal::addCMV);
        return retVal;
    }

    @Override
    public void add(AggResult r) {
        super.add(r);
        ((BoltAggResult) r).tupleProcess.forEach((s, cntMeanVar) ->
                this.tupleProcess.computeIfAbsent(s, (k) -> new CntMeanVar()).addCMV(cntMeanVar));
        ((BoltAggResult) r).getSheddingCountMap().forEach((stream,count)->{
            if(this.sheddingCountMap.containsKey(stream)){
                sheddingCountMap.put(stream,sheddingCountMap.get(stream)+count);
            }else{
                sheddingCountMap.put(stream,count);
            }
        });
        System.out.println("heiheihei"+sheddingCountMap);
    }

    public double getAvgServTimeHis(){
        return this.getCombinedProcessedResult().getAvg();
    }

    public double getScvServTimeHis(){
        return this.getCombinedProcessedResult().getScv();
    }

    public long getNumCompleteTuples(){
        return this.getCombinedProcessedResult().getCount();
    }

    public Map<String, Long> getSheddingCountMap() {
        return sheddingCountMap;
    }


    public static void main(String[] args) {
        List<String> list = new ArrayList<String>();
        list.add( "0" );
        list.add( "1" );
        list.add( "2" );
        list.add( "3" );
        list.add( "4" );
        list.add( "5" );
        list.add( "6" );
        list.add( "7" );
        list.add( "8" );
        list.add( "9" );
        list.add( "10" );
        list.add( "11" );
        list.add( "12" );
        list.add( "13" );
        list.add( "14" );
        list.add( "15" );
        list.add( "16" );
        list.add( "17" );
        list.add( "18" );
        list.add( "19" );
        list.add( "20" );
        System.out.println(list.size());
        int i = 0;
        Iterator<String> it = list.iterator();
        while ( it.hasNext() ) {
            String str = it.next();
            if ( i % 2 == 0 ) {
                //System.out.println( i + "===" + str );
                it.remove();
            }
            i++;
        }
        System.out.println(list.size());
        for ( String str : list ) {
           // System.out.println( str );
        }
        HashMap<String,Integer> s = new HashMap<>();
        s.put("1",100);s.put("2",1);s.put("3",1);s.put("4",1);s.put("5",1);s.put("6",1);s.put("7",1);s.put("8",1);
        System.out.println(s.values().stream().mapToInt(Number::intValue).sum());
        System.out.println(102<<1);
    }
}
