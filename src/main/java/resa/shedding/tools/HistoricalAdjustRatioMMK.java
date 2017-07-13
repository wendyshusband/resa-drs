package resa.shedding.tools;


import org.javatuples.Pair;
import resa.util.FixedSizeQueue;

/**
 * Created by 44931 on 2017/7/7.
 */
public class HistoricalAdjustRatioMMK {

    private int historySize;
    public FixedSizeQueue historyAdjustRatioResults;


    public HistoricalAdjustRatioMMK(int historySize) {
        this.historySize = historySize;
        historyAdjustRatioResults = new FixedSizeQueue(historySize);
    }

    public void putResult(double realLatency, double estSojournTime) {
        //System.out.println(historySize+"haiwode");
        System.out.println(realLatency+"!!!"+estSojournTime);
        boolean a = historyAdjustRatioResults.add(new Pair<>(estSojournTime, realLatency));
        //System.out.println(historyAdjustRatioResults.size()+"wobuxinshilinga"+a);
    }

    public void clear() {
        historyAdjustRatioResults.clear();
    }

}
