package TestTopology.fpsleep;

import TestTopology.fp.*;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import resa.shedding.basicServices.SheddingResaTopologyBuilder;
import resa.shedding.tools.TestRedis;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * Created by ding on 14-6-6.
 */
public class FrequentPatternTopologySleep implements Constant {

    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);
        int checktype = Integer.valueOf((Integer) conf.get("test.shedding.or.not"));
        TopologyBuilder builder = null;
        if (checktype == 0) {
            builder = new TopologyBuilder();
            System.out.println("fp origin storm");
        } else if (checktype == 1) {
            builder = new ResaTopologyBuilder();
            System.out.println("fp origin drs");
        } else if (checktype == 2) {
            builder = new SheddingResaTopologyBuilder();
            System.out.println("fp shedding drs");
        }
        int numWorkers = ConfigUtil.getInt(conf, "fp-worker.count", 1);
        resaConfig.setNumWorkers(numWorkers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");

        double generator_mu = ConfigUtil.getDouble(conf, "fp.generator.mu", 1000.0);
        double detector_mu = ConfigUtil.getDouble(conf, "fp.detector.mu", 1000.0);
        double reporter_mu = ConfigUtil.getDouble(conf, "fp.reporter.mu", 1000.0);
        System.out.println("mudedajihe: "+generator_mu+"~~~"+detector_mu+"~~~"+reporter_mu);
        builder.setSpout("input", new SentenceSpout(host, port, queue), ConfigUtil.getInt(conf, "fp.spout.parallelism", 1));

        builder.setBolt("generator", new PatternGeneratorSleep(() -> (long) (-Math.log(Math.random()) * 1000.0 / generator_mu)), ConfigUtil.getInt(conf, "fp.generator.parallelism", 1))
                .shuffleGrouping("input")
                .setNumTasks(ConfigUtil.getInt(conf, "fp.generator.tasks", 1));
        builder.setBolt("detector", new DetectorSleep(() -> (long) (-Math.log(Math.random()) * 1000.0 / detector_mu)), ConfigUtil.getInt(conf, "fp.detector.parallelism", 1))
                .directGrouping("generator")
                .directGrouping("detector", FEEDBACK_STREAM)
                .setNumTasks(ConfigUtil.getInt(conf, "fp.detector.tasks", 1));

        builder.setBolt("reporter", new PatternReporterSleep(() -> (long) (-Math.log(Math.random()) * 1000.0 / reporter_mu)), ConfigUtil.getInt(conf, "fp.reporter.parallelism", 1))
                .fieldsGrouping("detector", REPORT_STREAM, new Fields(PATTERN_FIELD))
                .setNumTasks(ConfigUtil.getInt(conf, "fp.reporter.tasks", 1));

        if (ConfigUtil.getBoolean(conf, "fp.metric.resa", true)) {
            if (checktype == 1) {
                resaConfig.addDrsSupport();
                resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
                System.out.println("fp ResaMetricsCollector is registered");
            } else if (checktype == 2) {
                resaConfig.addSheddingSupport();
                resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
                System.out.println("fp shedding ResaMetricsCollector is registered");
            }
        }

//        if (ConfigUtil.getBoolean(conf, "fp.metric.redis", true)) {
//            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
//            System.out.println("RedisMetricsCollector is registered");
//        }
        //resaConfig.setDebug(true);
        TestRedis.add("type", "fp");
        TestRedis.add("rebalance","0");
        TestRedis.add("time", String.valueOf(0));
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
        //LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology(args[0], resaConfig, builder.createTopology());
        //Utils.sleep(1000000000);
    }

}
