package TestTopology.fp;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import resa.shedding.basicServices.SheddingResaTopologyBuilder;
import resa.shedding.tools.TestRedis;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * Created by ding on 14-6-6.
 */
public class FrequentPatternTopology implements Constant {

    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);
        //TopologyBuilder builder = new TopologyBuilder();
        //TopologyBuilder builder = new WritableTopologyBuilder();
        //TopologyBuilder builder = new ResaTopologyBuilder();
        TopologyBuilder builder = new SheddingResaTopologyBuilder();
        int numWorkers = ConfigUtil.getInt(conf, "fp-worker.count", 1);
        resaConfig.setNumWorkers(numWorkers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");

        builder.setSpout("input", new SentenceSpout(host, port, queue), ConfigUtil.getInt(conf, "fp.spout.parallelism", 1));

        builder.setBolt("generator", new PatternGenerator(), ConfigUtil.getInt(conf, "fp.generator.parallelism", 1))
                .shuffleGrouping("input")
                .setNumTasks(ConfigUtil.getInt(conf, "fp.generator.tasks", 1));
        builder.setBolt("detector", new Detector(), ConfigUtil.getInt(conf, "fp.detector.parallelism", 1))
                .directGrouping("generator")
                //.directGrouping("detector", FEEDBACK_STREAM)
                .setNumTasks(ConfigUtil.getInt(conf, "fp.detector.tasks", 1));

        builder.setBolt("reporter", new PatternReporter(), ConfigUtil.getInt(conf, "fp.reporter.parallelism", 1))
                .fieldsGrouping("detector", REPORT_STREAM, new Fields(PATTERN_FIELD))
                .setNumTasks(ConfigUtil.getInt(conf, "fp.reporter.tasks", 1));

        if (ConfigUtil.getBoolean(conf, "fp.metric.resa", false)) {
            resaConfig.addSheddingSupport();//.addOptimizeSupport();
            //resaConfig.addDrsSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

//        if (ConfigUtil.getBoolean(conf, "fp.metric.redis", true)) {
//            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
//            System.out.println("RedisMetricsCollector is registered");
//        }
        //resaConfig.setDebug(true);
        TestRedis.add("type", "fp");
        TestRedis.add("time", String.valueOf(0));
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
        //LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology(args[0], resaConfig, builder.createTopology());
        //Utils.sleep(1000000000);
    }

}
