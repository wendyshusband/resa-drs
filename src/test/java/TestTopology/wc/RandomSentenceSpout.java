package TestTopology.wc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import resa.shedding.tools.FrequencyRestrictor;
import resa.util.ConfigUtil;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomSentenceSpout extends BaseRichSpout {

    public static String[] SENTENCES = new String[]{
            "over the moon"
//            "the cow jumped over the moon",
//            "an apple a day keeps the doctor away",
//            "four score and seven years ago",
//            "snow white and the seven dwarfs",
//            "i am at two with nature",
//            "the latest news from Yahoo! news",
//            "breaking news latest news and current news",
//            "the latest news from across canada and around the world",
//            "get top headlines on international business news",
//            "cnn delivers the latest breaking news and information on the latest top stories",
//            "get breaking national and world news broadcast video coverage and exclusive interviews"
    };

    private static final long serialVersionUID = 3963979649966518694L;

    private transient SpoutOutputCollector _collector;
    private transient Random _rand;
    private FrequencyRestrictor frequencyRestrictor;
    private AtomicInteger co = new AtomicInteger(0);
    private int number;
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        frequencyRestrictor = new FrequencyRestrictor(ConfigUtil.getInt(conf, "maxFrequencyPerSecond", 500),
                ConfigUtil.getInt(conf, "windowsPerSecond", 500));
        number = ConfigUtil.getInt(conf, "wc-number", 10000);
    }

    @Override
    public void nextTuple() {
        if (frequencyRestrictor.tryPermission() && co.get() < number) {
            co.getAndIncrement();
            String sentence = SENTENCES[_rand.nextInt(SENTENCES.length)];
            _collector.emit(new Values(sentence), UUID.randomUUID().toString());
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
    }

}