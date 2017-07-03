package TestTopology.sort;

import TestTopology.simulated.TASleepBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by kailin on 11/4/17.
 */
public class SortWorkBolt2Path extends TASleepBolt {
    OutputCollector _collector;
    String cha;
    public SortWorkBolt2Path(String cha){
        this.cha = cha;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        Utils.sleep(5);
        String sid = tuple.getString(0);
        Random random = new Random();
        int prob = random.nextInt(2);
        String sentence = tuple.getString(1)+cha+prob;
        if (prob == 0){
            _collector.emit("C-Stream", new Values(sid,sentence));
        }else{
            _collector.emit("D-Stream", new Values(sid,sentence));
        }
        _collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("C-Stream",new Fields("id",cha));
        outputFieldsDeclarer.declareStream("D-Stream",new Fields("id",cha));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
