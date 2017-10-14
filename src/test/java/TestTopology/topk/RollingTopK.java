package TestTopology.topk;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * Created by kailin on 23/5/17.
 */
public class RollingTopK {

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        String topologyName = args[0];

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");
        int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 5);
        int TOP_N = ConfigUtil.getInt(conf, "tk.topnumber", 3);

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);
        //TopologyBuilder builder = new TopologyBuilder();
        //TopologyBuilder builder = new WritableTopologyBuilder();
        TopologyBuilder builder = new ResaTopologyBuilder();
        //TopologyBuilder builder = new SheddingResaTopologyBuilder();
        builder.setSpout("wordGenerator", new StableFrequencyWordSpout(host, port, queue, true), ConfigUtil.getInt(conf, "tk.spout.parallelism", 1));
        builder.setBolt("counter",
                new RollingCountBolt(ConfigUtil.getInt(conf, "windowLengthInSeconds", 9), ConfigUtil.getInt(conf, "emitFrequencyInSeconds", 3)),
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
        resaConfig.setNumWorkers(ConfigUtil.getInt(conf, "tk-NumOfWorkers", 1));
        //conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "tk-MaxSpoutPending", 0));
        resaConfig.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        resaConfig.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        resaConfig.addDrsSupport();
        resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
        System.out.println("ResaMetricsCollector is registered");

        StormSubmitter.submitTopology(topologyName, resaConfig, builder.createTopology());
        //LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology(topologyName, conf, builder.createTopology());
        //Thread.sleep(10000000);
    }
}


