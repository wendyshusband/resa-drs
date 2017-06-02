package TestTopology.wordcount2;

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
import resa.util.ConfigUtil;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kailin on 1/6/17.
 */
public class CountWord {
    public static class Split extends BaseRichBolt{
        private static final long serialVersionUID = 9182719843878455933L;
        private OutputCollector collector;
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
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static class Count extends BaseRichBolt {
        private static final long serialVersionUID = 4905347436083499207L;
        private Map<String, Integer> counters;
        private int taskid;
        private OutputCollector collector;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            taskid = topologyContext.getThisTaskId();
            collector = outputCollector;
            counters = new HashMap<>();
        }

        @Override
        public void execute(Tuple tuple) {
            String str = tuple.getString(0);
            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
            collector.ack(tuple);
        }

        @Override
        public void cleanup() {
            System.out.println("Word Counter cleanup");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[0]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WordReader(), ConfigUtil.getInt(conf, "spout.parallelism", 1));
        builder.setBolt("split", new Split(), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .shuffleGrouping("spout");
        builder.setBolt("counter", new Count(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .shuffleGrouping("split");

        conf.setNumWorkers(ConfigUtil.getInt(conf, "wc-NumOfWorkers", 1));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));
        StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
    }
}
