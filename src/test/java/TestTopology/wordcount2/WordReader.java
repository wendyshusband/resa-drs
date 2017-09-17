package TestTopology.wordcount2;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.shedding.tools.FrequencyRestrictor;
import resa.util.ConfigUtil;

import java.util.Map;
import java.util.UUID;

/**
 * Created by kailin on 1/6/17.
 */
public class WordReader extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(WordReader.class);
    private static final long serialVersionUID = 3963979649166518694L;
    private SpoutOutputCollector collector;
    private FrequencyRestrictor frequencyRestrictor;
    private static int co = 0;
    private int number;
    public static String SENTENCES;
    private long startTime;
    private long timeSpan;
    private int maxFrequencyPerSecond;
    private int windowsPerSecond;
    private double times;
    private int count = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        startTime = System.currentTimeMillis();
        this.collector = collector;
        maxFrequencyPerSecond = ConfigUtil.getInt(conf, "maxFrequencyPerSecond", 500);
        windowsPerSecond = ConfigUtil.getInt(conf, "windowsPerSecond", 500);
        frequencyRestrictor = new FrequencyRestrictor(maxFrequencyPerSecond, windowsPerSecond);
        number = ConfigUtil.getInt(conf, "wc-number", 10000);
        SENTENCES = ConfigUtil.getString(conf, "wc_sentence", "over the moon");
        timeSpan = ConfigUtil.getInt(conf, "wc_timespan", 300) * 1000;
        times = ConfigUtil.getInt(conf, "wc_times", 2);
        LOG.info("heigezhiai word reader is created " + maxFrequencyPerSecond + "~" + windowsPerSecond + '~' + times + "~" + timeSpan);
    }

    @Override
    public void nextTuple() {
        if (System.currentTimeMillis() - startTime >= timeSpan) {
            //System.out.println(maxFrequencyPerSecond+"~~heihei~~"+times);
            maxFrequencyPerSecond += 100;
            windowsPerSecond += 100;
            frequencyRestrictor = new FrequencyRestrictor(maxFrequencyPerSecond, windowsPerSecond);
            LOG.info(System.currentTimeMillis() + "heigezhiai test time enough, we will change the frequency restrictor: " + maxFrequencyPerSecond);

            count++;
            if (count >= 3) {
                startTime = Long.MAX_VALUE;
                LOG.info(System.currentTimeMillis() + "heigezhiai" + count + " starttime" + startTime);
            } else {
                LOG.info(System.currentTimeMillis() + "heigezhiai" + count + " starttime" + startTime);
                startTime = System.currentTimeMillis();
            }
        }

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