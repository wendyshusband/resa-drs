package TestTopology.testforls;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import resa.shedding.tools.FrequencyRestrictor;

import java.util.Map;

/**
 * Created by kailin on 22/5/17.
 */
public class TestAccuracySpout extends BaseRichSpout {
    private FrequencyRestrictor frequencyRestrictor;
    SpoutOutputCollector _collector;
    private transient long count = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        frequencyRestrictor = new FrequencyRestrictor(500, 500);
    }

    public void nextTuple() {
        if(frequencyRestrictor.tryPermission()) {
            String id = "tuple"+count;
            count++;
            //Utils.sleep(5);
            _collector.emit(new Values(id, count), id);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id","count"));
    }

}
