package TestTopology.topk;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import resa.topology.RedisQueueSpout;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by kailin on 23/5/17.
 */
public class TopWordSpout extends RedisQueueSpout {
    boolean _isDistributed;


    public TopWordSpout(String host, int port, String queue, boolean isDistributed) {
        super(host, port, queue);
        this._isDistributed = isDistributed;
    }

    @Override
    protected void emitData(Object data) {
        String[] sentenceSplit = ((String) data).split(" ");
        for (int i=0; i<sentenceSplit.length; i++){
            collector.emit(new Values(sentenceSplit[i]), UUID.randomUUID().toString());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields(new String[]{"word"}));
        declarer.declare(new Fields("word"));
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
