package TestTopology.TestPassiveShedding;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by 44931 on 2017/6/9.
 */
public class Output2 extends BaseRichBolt {
    private OutputCollector collector;
    private int num=0;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
//        if(tuple.getIntegerByField("sub") != 0){
//            num++;
//        }
        //System.out.println(num+"hehehe");
        collector.emit(tuple,new Values(tuple.getString(0)));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("output"));
    }
}