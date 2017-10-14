package TestTopology.wc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import resa.util.ConfigUtil;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

    public static class SplitSentence extends BaseRichBolt {

        private static final long serialVersionUID = 9182719848878455933L;
        private OutputCollector collector;
        public SplitSentence() {
        }

//        @Override
//        public void execute(Tuple input, BasicOutputCollector collector) {
//            //Utils.sleep(1);
//            String sentence = input.getStringByField("sentence");
//            StringTokenizer tokenizer = new StringTokenizer(sentence.replaceAll("\\p{P}|\\p{S}", " "));
//            while (tokenizer.hasMoreTokens()) {
//                String word = tokenizer.nextToken().trim();
//                if (!word.isEmpty()) {
//                    collector.emit(Arrays.asList((Object) word.toLowerCase()));
//                }
//            }
//        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

//        @Override
//        public Map<String, Object> getComponentConfiguration() {
//            return null;
//        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] sentenceSplit = sentence.split(" ");
            for (int i=0; i<sentenceSplit.length; i++){
                collector.emit(tuple,new Values(sentenceSplit[i]));
            }
//            StringTokenizer tokenizer = new StringTokenizer(sentence.replaceAll("\\p{P}|\\p{S}", " "));
//            while (tokenizer.hasMoreTokens()) {
//                String word = tokenizer.nextToken().trim();
//                if (!word.isEmpty()) {
//                    collector.emit(tuple, new Values(word.toLowerCase()));
//                }
//            }
            collector.ack(tuple);
        }

        @Override
        public void cleanup() {
            System.out.println("Split cleanup");
        }
    }

    public static class WordCount extends BaseRichBolt {
        private static final long serialVersionUID = 4905347466083499207L;
        private int numBuckets = 6;
        private Map<String, Integer> counters;
        private int taskid;
        private OutputCollector collector;
//        @Override
//        public void prepare(Map stormConf, TopologyContext context) {
//            taskid = context.getThisTaskId();
//            super.prepare(stormConf, context);
//            counters = (Map<String, Integer>) context.getTaskData("words");
//            if (counters == null) {
//                counters = new HashMap<>();
//                context.setTaskData("words", counters);
//            }
//            int interval = Utils.getInt(stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
//            //context.registerMetric("number-words", this::getNumWords, interval);
//        }

        private long getNumWords() {
            //counters.rotate();
            return counters.size();
        }

//        @Override
//        public void execute(Tuple tuple, BasicOutputCollector collector) {
//            //Utils.sleep(1);
//            String word = tuple.getStringByField("word");
//            Integer count = counters.get(word);
//            if (count == null) {
//                count = 0;
//            }
//            count++;
//            counters.put(word, count);
//            //if(count > 9990)
//                System.out.println(taskid+"output result: "+word+" : "+count);
//            collector.emit(new Values(word, count));
//
//        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            taskid = topologyContext.getThisTaskId();
            collector = outputCollector;
            counters = (Map<String, Integer>) topologyContext.getTaskData("words");
            if (counters == null) {
                counters = new HashMap<>();
                topologyContext.setTaskData("words", counters);
            }
            int interval = Utils.getInt(map.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
            //context.registerMetric("number-words", this::getNumWords, interval);
        }

        @Override
        public void execute(Tuple tuple) {
            //Utils.sleep(1);
            String word = tuple.getStringByField("word");
            Integer count = counters.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counters.put(word, count);
            //if(count > 9990)
                System.out.println(taskid+"output result:"+word+":"+count);
            //collector.emit(new Values(word, count));
            collector.ack(tuple);
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
                //.setNumTasks(defaultTaskNum)
                .shuffleGrouping("say");
        builder.setBolt("counter", new WordCount(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                //.setNumTasks(defaultTaskNum)
                .shuffleGrouping("split");
                //.fieldsGrouping("split", new Fields("word"));
       // builder.setBolt("rec",new outputBolt(),1).shuffleGrouping("counter");
        conf.setNumWorkers(ConfigUtil.getInt(conf, "wc-NumOfWorkers", 1));
        //conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "wc-MaxSpoutPending", 0));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));
        //conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
    }
}
