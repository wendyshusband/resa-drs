package TestTopology.outdet;

import resa.shedding.tools.TestRedis;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by ding on 14-3-14.
 */
public class Updater implements IRichBolt {

    private OutputCollector collector;
    private Map<String, List<BitSet>> padding;
    private int projectionSize;

    public Updater(int projectionSize) {
        this.projectionSize = projectionSize;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        padding = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        //Utils.sleep(8);
        String key = input.getValueByField(ObjectSpout.TIME_FILED) + "-" + input.getValueByField(ObjectSpout.ID_FILED);
        List<BitSet> ret = padding.get(key);
        if (ret == null) {
            ret = new ArrayList<>();
            padding.put(key, ret);
        }
        ret.add((BitSet) input.getValueByField(Detector.OUTLIER_FIELD));
        if (ret.size() == projectionSize) {
            padding.remove(key);
            BitSet result = ret.get(0);
            ret.stream().forEach((bitSet) -> {
                if (result != bitSet) {
                    result.or(bitSet);
                }
            });
            // output
           //System.out.println("result~"+result.size());
            TestRedis.insertList("full",result.toString());
            result.stream().forEach((status) -> {
                if (status == 0) {
                    // output
                    //System.out.println("re"+result);
                    //TestRedis.insertList("status0",result.toString());
                    //collector.emit(new Values());
                }
            });
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
