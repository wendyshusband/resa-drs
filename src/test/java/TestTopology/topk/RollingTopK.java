package TestTopology.topk;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import resa.util.ConfigUtil;

import java.io.File;

/**
 * Created by kailin on 23/5/17.
 */
public class RollingTopK {

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[0]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }
        String topologyName = "slidingWindowCounts";
        if (args.length >= 1) {
            topologyName = args[1];
        }
        int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 5);
        int TOP_N = ConfigUtil.getInt(conf, "tk.topnumber", 1);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordGenerator", new TopWordSpout(), ConfigUtil.getInt(conf, "tk.spout.parallelism", 1));
        builder.setBolt("counter", new RollingCountBolt(9, 3),
                ConfigUtil.getInt(conf, "tk.counter.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("wordGenerator", new Fields("word"));
        builder.setBolt("intermediateRanker", new IntermediateRankingsBolt(TOP_N),
                ConfigUtil.getInt(conf, "tk.intermediateRanker.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("counter", new Fields("obj"));
        builder.setBolt("finalRanker", new TotalRankingsBolt(TOP_N))
                .setNumTasks(defaultTaskNum)
                .globalGrouping("intermediateRanker");
        conf.setNumWorkers(ConfigUtil.getInt(conf, "tk-NumOfWorkers", 1));
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "tk-MaxSpoutPending", 0));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
}


