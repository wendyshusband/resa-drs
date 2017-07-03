package TestTopology.sort;

import TestTopology.simulated.TASleepBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kailin on 11/4/17.
 */
public class SortWorkBolt extends TASleepBolt {
        OutputCollector _collector;
        String cha;
        public SortWorkBolt(String cha){
            this.cha = cha;
        }
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String sentence = tuple.getString(0)+cha;
            _collector.emit(tuple, new Values(sentence.toUpperCase()));
            _collector.emit(tuple,new Values(sentence.toLowerCase()));
            _collector.ack(tuple);
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(cha));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
}
