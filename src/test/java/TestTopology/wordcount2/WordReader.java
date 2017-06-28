package TestTopology.wordcount2;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import resa.shedding.tools.FrequencyRestrictor;
import resa.util.ConfigUtil;

import java.util.Map;
import java.util.UUID;

/**
 * Created by kailin on 1/6/17.
 */
public class WordReader extends BaseRichSpout {
    private static final long serialVersionUID = 3963979649166518694L;
    private SpoutOutputCollector collector;
    private FrequencyRestrictor frequencyRestrictor;
    private static int co = 0;
    private int number;
    public static String SENTENCES;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        frequencyRestrictor = new FrequencyRestrictor(ConfigUtil.getInt(conf, "maxFrequencyPerSecond", 500),
                ConfigUtil.getInt(conf, "windowsPerSecond", 500));
        number = ConfigUtil.getInt(conf, "wc-number", 10000);
        SENTENCES = ConfigUtil.getString(conf,"wc_sentence","over the moon");
    }

    @Override
    public void nextTuple() {
        if (frequencyRestrictor.tryPermission() && co < number) {
            co++;
            String sentence = SENTENCES;
            collector.emit(new Values(sentence), UUID.randomUUID().toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}