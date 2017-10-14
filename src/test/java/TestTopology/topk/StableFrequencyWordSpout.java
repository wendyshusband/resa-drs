package TestTopology.topk;

import TestTopology.helper.RedisQueueSpout2;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by 44931 on 2017/9/13.
 */
public class StableFrequencyWordSpout extends RedisQueueSpout2 {
    boolean _isDistributed;
    private static final long start = System.currentTimeMillis();

    public StableFrequencyWordSpout(String host, int port, String queue, boolean isDistributed) {
        super(host, port, queue);
        this._isDistributed = isDistributed;
    }

    @Override
    protected void emitData(Object data) {
        System.out.println(data+"~~~~~~~~~~~~~~~~~~~~~~~~~");
        collector.emit(new Values(data), UUID.randomUUID().toString());
//        String[] sentenceSplit = ((String) data).split(" ");
//        for (int i=0; i<sentenceSplit.length; i++){
//            collector.emit(new Values(sentenceSplit[i]));
//        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[]{"word"}));
        //declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        if(!this._isDistributed) {
            HashMap ret = new HashMap();
            ret.put("topology.max.task.parallelism", Integer.valueOf(1));
            return ret;
        } else {
            return null;
        }
    }

    public void close() {
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }
}
