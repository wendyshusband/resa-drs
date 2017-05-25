package TestTopology.topk;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import resa.shedding.tools.FrequencyRestrictor;
import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kailin on 23/5/17.
 */
public class TopWordSpout extends BaseRichSpout{
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    private FrequencyRestrictor frequencyRestrictor;
    private AtomicInteger co = new AtomicInteger(0);
    private int number;
    public TopWordSpout() {
        this(true);
    }

    public TopWordSpout(boolean isDistributed) {
        this._isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        frequencyRestrictor = new FrequencyRestrictor(ConfigUtil.getInt(conf, "maxFrequencyPerSecond", 50000),
                ConfigUtil.getInt(conf, "windowsPerSecond", 500));
        this._collector = collector;
        number = ConfigUtil.getInt(conf, "wc-number", 10000);
    }

    public void close() {
    }

    public void nextTuple() {
        //Utils.sleep(100L);
        if (frequencyRestrictor.tryPermission()&& co.get() < number){
            co.getAndIncrement();
            String[] words = new String[]{"nathan","nathan","nathan","nathan","nathan", "mike", "mike","jackson","jackson","jackson", "golda","golda","golda","golda"};
            //Random rand = new Random();
            for(int i=0; i<words.length;i++) {
                String word = words[i];
                this._collector.emit(new Values(new Object[]{word}), UUID.randomUUID().toString());
            }
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[]{"word"}));
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
}
