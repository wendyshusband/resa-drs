package TestTopology.wc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import resa.util.ConfigUtil;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

    public static class SplitSentence extends BaseBasicBolt {

        private static final long serialVersionUID = 9182719848878455933L;

        public SplitSentence() {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getStringByField("sentence");
            StringTokenizer tokenizer = new StringTokenizer(sentence.replaceAll("\\p{P}|\\p{S}", " "));
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().trim();
                if (!word.isEmpty()) {
                    collector.emit(Arrays.asList((Object) word.toLowerCase()));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public void cleanup() {
            System.out.println("Split cleanup");
        }
    }

    public static class WordCount extends BaseBasicBolt {
        private static final long serialVersionUID = 4905347466083499207L;
        private int numBuckets = 6;
        private Map<String, Integer> counters;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);
            counters = (Map<String, Integer>) context.getTaskData("words");
            if (counters == null) {
                counters = new HashMap<>();
                context.setTaskData("words", counters);
            }
            int interval = Utils.getInt(stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
            //context.registerMetric("number-words", this::getNumWords, interval);
        }

        private long getNumWords() {
            //counters.rotate();
            return counters.size();
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Utils.sleep(10);
            String word = tuple.getStringByField("word");
            Integer count = counters.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counters.put(word, count);
            System.out.println("haonanshou: "+word+" : "+count);
            //collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void cleanup() {
                System.out.println("Word Counter cleanup");
        }

    }

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[0]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }
        TopologyBuilder builder = new TopologyBuilder();
        int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 5);
        if (!ConfigUtil.getBoolean(conf, "spout.redis", false)) {
            builder.setSpout("say", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "spout.parallelism", 1));
        } else {
            String host = (String) conf.get("redis.host");
            int port = ((Number) conf.get("redis.port")).intValue();
            String queue = (String) conf.get("redis.queue");
            builder.setSpout("say", new RedisSentenceSpout(host, port, queue),
                    ConfigUtil.getInt(conf, "spout.parallelism", 1));
        }
        builder.setBolt("split", new SplitSentence(), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("say");
        builder.setBolt("counter", new WordCount(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("split", new Fields("word"));

        conf.setNumWorkers(ConfigUtil.getInt(conf, "wc-NumOfWorkers", 1));
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "wc-MaxSpoutPending", 0));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
    }
}
