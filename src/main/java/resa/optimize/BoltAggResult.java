package resa.optimize;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-5-6.
 */
public class BoltAggResult extends AggResult {
    private Map<String, CntMeanVar> sheddingProcess = new HashMap<>();//tkl
    private Map<String, CntMeanVar> tupleProcess = new HashMap<>();


    public Map<String, CntMeanVar> getSheddingProcess() {
        return sheddingProcess;
    }

    public Map<String, CntMeanVar> getTupleProcess() {
        return tupleProcess;
    }

    public CntMeanVar getCombinedProcessedResult() {
        CntMeanVar retVal = new CntMeanVar();
        tupleProcess.values().stream().forEach(retVal::addCMV);
        return retVal;
    }

    public CntMeanVar getCombinedSheddingProcessedResult() {//tkl
        CntMeanVar retVal = new CntMeanVar();
        sheddingProcess.values().stream().forEach(retVal::addCMV);
        return retVal;
    }
    @Override
    public void add(AggResult r) {
        super.add(r);
        ((BoltAggResult) r).tupleProcess.forEach((s, cntMeanVar) ->
                this.tupleProcess.computeIfAbsent(s, (k) -> new CntMeanVar()).addCMV(cntMeanVar));
        ((BoltAggResult) r).sheddingProcess.forEach((s, cntMeanVar) ->//tkl
                this.sheddingProcess.computeIfAbsent(s, (k) -> new CntMeanVar()).addCMV(cntMeanVar));
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

    public double getAvgSheddingRateHis(){
        return this.getCombinedSheddingProcessedResult().getAvg();
    }//tkl

}
