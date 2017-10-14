package TestTopology.wordcount2;

import TestTopology.TestPassiveShedding.Output2;
import TestTopology.helper.IntervalSupplier;
import TestTopology.simulated.TASleepBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
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
    public static class Split extends TASleepBolt {
        private static final long serialVersionUID = 9182719843878455933L;
        private OutputCollector collector;

        public Split(IntervalSupplier sleep){
            super(sleep);
        }
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            super.execute(tuple);
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

    public static class Count extends TASleepBolt {
        private static final long serialVersionUID = 4905347436083499207L;
        private Map<String, Integer> counters;
        private int taskid;
        private OutputCollector collector;

        public Count(IntervalSupplier sleep){
            super(sleep);
        }
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            taskid = topologyContext.getThisTaskId();
            collector = outputCollector;
            counters = new HashMap<>();
        }

        @Override
        public void execute(Tuple tuple) {
            super.execute(tuple);
            String str = tuple.getString(0);
            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
            collector.emit(tuple, new Values(str));
            collector.ack(tuple);
        }

        @Override
        public void cleanup() {
            System.out.println("Word Counter cleanup");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("map"));
        }
    }

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        TopologyBuilder builder = new TopologyBuilder();
        int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 10);

        builder.setSpout("spout", new WordReader(), ConfigUtil.getInt(conf, "spout.parallelism", 1))
                ;//.setNumTasks(3);
        double split_mu = ConfigUtil.getDouble(conf, "split.mu", 1.0);

        builder.setBolt("split", new Split(() -> (long) (-Math.log(Math.random()) * 1000.0 / split_mu)), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("spout");
        double count_mu = ConfigUtil.getDouble(conf, "count.mu", 1.0);

        builder.setBolt("counter", new Count(() -> (long) (-Math.log(Math.random()) * 1000.0 / count_mu)), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("split", new Fields("word"));
        builder.setBolt("out", new Output2(() -> 1L),1)
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("counter");
        conf.setNumWorkers(ConfigUtil.getInt(conf, "wc-NumOfWorkers", 1));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("test", conf, builder.createTopology());
        //Utils.sleep(10000);
    }
}
