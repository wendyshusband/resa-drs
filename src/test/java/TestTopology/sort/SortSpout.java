package TestTopology.sort;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import resa.shedding.tools.FrequencyRestrictor;
import resa.util.ConfigUtil;

import java.util.Map;

/**
 * Created by kailin on 11/4/17.
 */
public class SortSpout extends BaseRichSpout {
        private FrequencyRestrictor frequencyRestrictor;
        boolean _isDistributed;
        SpoutOutputCollector _collector;
        private String spoutIdPrefix;
        private transient long count = 0;
        private int number;
        public SortSpout() {
            this(true,"spOut");
        }

        public SortSpout(boolean isDistributed, String prefix) {
            _isDistributed = isDistributed;
            spoutIdPrefix = prefix;
        }

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            _collector = spoutOutputCollector;
            frequencyRestrictor = new FrequencyRestrictor(ConfigUtil.getInt(map, "maxFrequencyPerSecond", 500),
                    ConfigUtil.getInt(map, "windowsPerSecond", 500));
            number = ConfigUtil.getInt(map, "wc-number", 10000);
        }

        public void nextTuple() {
            if(frequencyRestrictor.tryPermission() && count < number) {
                String id = spoutIdPrefix + count;
                count++;
                _collector.emit(new Values("TUpLE"), id);
                //_collector.emit(new Values(id, "TUPLE"));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(spoutIdPrefix));
        }
}
