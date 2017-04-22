package TestTopology.simulated;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kailin on 11/4/17.
 */
public class SortSpout extends BaseRichSpout {
        boolean _isDistributed;
        SpoutOutputCollector _collector;
        private String spoutIdPrefix;
        private transient long count = 0;
        public SortSpout() {
            this(true,"spOut");
        }

        public SortSpout(boolean isDistributed,String prefix) {
            _isDistributed = isDistributed;
            spoutIdPrefix = prefix;
        }

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            _collector = spoutOutputCollector;
        }

        public void nextTuple() {
            String id = spoutIdPrefix + count;
            count++;
            for(int i=0;i<1000000;i++){
                double j =  Math.log(i+100);
                j++;
                j--;
                j=j+1;
                j=j-1;
            }
            _collector.emit(new Values(id,"TUPLE"),id);

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id",spoutIdPrefix));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if(!_isDistributed){
                Map<String, Object> ret = new HashMap<String, Object>();
                ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
                return ret;
            }else{
                return null;
            }

        }
}
