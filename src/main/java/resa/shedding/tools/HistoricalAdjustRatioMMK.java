package resa.shedding.tools;


import org.javatuples.Pair;
import resa.util.FixedSizeQueue;

/**
 * Created by kailin on 2017/7/7.
 */
public class HistoricalAdjustRatioMMK {

    private Pair historyMean;
    //private int historyCountForMean;
    private int historySize;
    public FixedSizeQueue historyAdjustRatioResults;


    public HistoricalAdjustRatioMMK(int historySize) {
        this.historySize = historySize;
        historyAdjustRatioResults = new FixedSizeQueue(historySize);
    }

    public void putResult(double realLatency, double estSojournTime) {
        System.out.println(realLatency+"!!!"+estSojournTime);
       // historyAdjustRatioResults.toArray();
        historyAdjustRatioResults.add(new Pair<>(estSojournTime, realLatency));
    }

    public void clear() {
        historyAdjustRatioResults.clear();
    }

}
