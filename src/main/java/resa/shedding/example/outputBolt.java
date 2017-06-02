package resa.shedding.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kailin on 7/3/17.
 */
public class outputBolt extends BaseRichBolt {
    protected transient OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector =outputCollector;
    }

    public void execute(Tuple tuple) {
        //new TestPrint("tupleValue=",tuple.toString());
        collector.emit(tuple,new Values(tuple.getString(1)));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("final"));
    }
}
