package TestTopology.outdetsleep;

import TestTopology.helper.IntervalSupplier;
import TestTopology.simulated.TASleepBolt;
import resa.shedding.tools.TestRedis;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by kailin on 14-3-14.
 */
public class UpdaterSleep extends TASleepBolt {

    private OutputCollector collector;
    private Map<String, List<BitSet>> padding;
    private int projectionSize;
    private int taskid;

    public UpdaterSleep(int projectionSize, IntervalSupplier sleep) {
        super(sleep);
        this.projectionSize = projectionSize;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        padding = new HashMap<>();
        taskid = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input) {
        super.execute(input);
        //System.out.println("updaterfacaila:"+taskid);
        String key = input.getValueByField(ObjectSpoutSleep.TIME_FILED) + "-" + input.getValueByField(ObjectSpoutSleep.ID_FILED);
        List<BitSet> ret = padding.get(key);
        if (ret == null) {
            ret = new ArrayList<>();
            padding.put(key, ret);
        }
        ret.add((BitSet) input.getValueByField(DetectorSleep.OUTLIER_FIELD));
        if (ret.size() == projectionSize) {
            padding.remove(key);
            BitSet result = ret.get(0);
            ret.stream().forEach((bitSet) -> {
                if (result != bitSet) {
                    result.or(bitSet);
                }
            });
            // output
            Object jb = input.getValueByField(ObjectSpoutSleep.JB_FILED);
            //System.out.println(jb+"result~"+result.size());
            TestRedis.insertList("full", jb+":"+result.toString());
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
