package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tom.fu on 22/6/2016.
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-0.10.0
 * Functions involving queue-related metrics in the current class will be affected:
 *
 * TODO: the design and usage of this class is not clear, need to consider re-design in future.
 * TODO: this class needs improvement and redesign in the next version
 */
public class SourceNode {
    private static final Logger LOG = LoggerFactory.getLogger(SourceNode.class);

    private String componentID;
    private int executorNumber;
    private double compSampleRate;

    private double avgSendQueueLength;
    private double avgRecvQueueLength;

    private double realLatencyMilliSeconds;
    private double scvRealLatency;

    private double numCompleteTuples;
    private double sumDurationSeconds;
    private double tupleCompleteRate;

    /*metrics on send_queue*/
    private double tupleEmitRateOnSQ;
    private double tupleEmitScvByInterArrival;
    /*metrics on recv_queue*/
    private double exArrivalRate;
    private double exArrivalScvByInterArrival;

    /*load shedding*/
    protected Map<String, Long> emitCount = new HashMap<>();
    protected Map<String, Long> shedRelateCount = new HashMap<>();
    protected double dropRatio = 0.0;

    public SourceNode(String componentID, int executorNumber, double compSampleRate, SpoutAggResult ar, boolean enableLoadShedding){
        this.componentID = componentID;
        this.executorNumber = executorNumber;
        this.compSampleRate = compSampleRate;

        this.avgSendQueueLength = ar.getAvgSendQueueLength();
        this.avgRecvQueueLength = ar.getAvgRecvQueueLength();

        this.realLatencyMilliSeconds = ar.getAvgTupleCompleteLatency();
        this.scvRealLatency = ar.getScvTupleCompleteLatency();

        this.numCompleteTuples = ar.getNumOfCompletedTuples();
        this.sumDurationSeconds = ar.getDurationSeconds() / executorNumber;
        this.tupleCompleteRate = numCompleteTuples * executorNumber / (sumDurationSeconds * compSampleRate);

        /** the calculation on tupleEmitRate is affected by whether the acker is enabled **/
        double departRateHis = ar.getDepartureRatePerSec();
        /** assume acker is enabled, therefore, every tuple emitted by Spout, there is an acker tuple **/
        this.tupleEmitRateOnSQ = departRateHis * executorNumber / 2.0;
        this.tupleEmitScvByInterArrival = ar.getInterLeavelTimeScv();

        /** instead, we prefer to use the following calculation for external arrival rate **/
        double arrivalRateHis = ar.getArrivalRatePerSec();
        this.exArrivalRate = arrivalRateHis * executorNumber;
        this.exArrivalScvByInterArrival = ar.getInterArrivalTimeScv();
        if (enableLoadShedding) {
            this.emitCount = ar.getemitCount();//load shedding
            this.shedRelateCount = ar.getShedRelateCount();
            this.dropRatio = (this.shedRelateCount.get("spoutDrop")*1.0) / (this.emitCount.values().stream().mapToLong(Number::longValue).sum()+this.shedRelateCount.get("spoutDrop"));
        }

        LOG.info((ar.getDepartureRatePerSec()/2)+":"+executorNumber+"SourceNode is created: " + toString());
    }

    public double getDropRatio() {
        return dropRatio;
    }

    public String getComponentID() {
        return this.componentID;
    }

    public int getExecutorNumber() {
        return executorNumber;
    }

    public double getCompSampleRate() {
        return compSampleRate;
    }

    public double getAvgSendQueueLength(){
        return avgSendQueueLength;
    }

    public double getAvgRecvQueueLength(){
        return avgRecvQueueLength;
    }

    public double getRealLatencyMilliSeconds() {
        return realLatencyMilliSeconds;
    }

    public double getScvRealLatency(){
        return scvRealLatency;
    }

    public double getRealLatencySeconds() {
        return realLatencyMilliSeconds / 1000.0;
    }

    public double getNumCompleteTuples() {
        return numCompleteTuples;
    }

    public double getSumDurationMilliSeconds() {
        return sumDurationSeconds * 1000.0;
    }

    public double getSumDurationSeconds() {
        return sumDurationSeconds;
    }

    public double getTupleCompleteRate() {
        return tupleCompleteRate;
    }

    public double getTupleEmitRateOnSQ() {
        return tupleEmitRateOnSQ;
    }

    public double getTupleEmitScvByInterArrival() {
        return tupleEmitScvByInterArrival;
    }

    public double getExArrivalRate() {
        return exArrivalRate;
    }

    public double getExArrivalScvByInterArrival() {
        return exArrivalScvByInterArrival;
    }

    public Map<String, Long> getEmitCount() {//load shedding
        return emitCount;
    }

    public Map<String, Long> getShedRelateCount() {
        return shedRelateCount;
    }

    /**
     * revert complete latency for load shedding.
     * */
    public void revertCompleteLatency(double realCL) {
        this.realLatencyMilliSeconds = realCL;
    }

    @Override
    public String toString() {
        return String.format(
                "(ID, eNum):(%s,%d), FinRate: %.3f, avgCTime: %.3f, scvCTime: %.3f, FinCnt: %.1f, Dur: %.1f, sample: %.1f, SQLen: %.1f, RQLen: %.1f, " +
                        "-----> rateSQ: %.3f, rateSQScv: %.3f, eArr: %.3f, eArrScv: %.3f, dropRatio: %.3f",
                componentID, executorNumber, tupleCompleteRate, realLatencyMilliSeconds, scvRealLatency, numCompleteTuples,
                sumDurationSeconds, compSampleRate, avgSendQueueLength, avgRecvQueueLength,
                tupleEmitRateOnSQ, tupleEmitScvByInterArrival, exArrivalRate, exArrivalScvByInterArrival, dropRatio)
                +" emitcount: "+emitCount+" | "+this.shedRelateCount;
    }

    public void revertLambda(double lambda) {
        this.tupleEmitRateOnSQ = lambda;
        this.exArrivalRate = lambda;
    }
}
