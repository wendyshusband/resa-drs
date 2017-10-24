package TestTopology.outdetsleep;

import TestTopology.testforls.TestWRInputFileForRedis;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import resa.shedding.basicServices.SheddingResaTopologyBuilder;
import resa.shedding.tools.TestRedis;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

//import resa.metrics.RedisMetricsCollector;

/**
 * Created by kailin on 14-3-17.
 */
public class OutlierDetectionTopology {

    @Test
    public void add() {
        Jedis jedis = TestRedis.getJedis();
        List<double[]> v = OutlierDetectionTopology.generateRandomVectors(34,13);
        for (int i=0; i<v.size(); i++) {
            System.out.println(v.size()+"~"+v.get(i).length);
            for (int j=0; j<v.get(i).length; j++) {
                jedis.lpush("vector", String.valueOf(v.get(i)[j]));
            }
        }
//        List<double[]> v = getDefineVectors();
//        System.out.println(v.size());
//        List<double[]> v = new ArrayList<>();
//        for (int k=0; k<=2; k++) {
//            for (int i = 0; i < 5; i++) {
//                double[] temp = new double[34];
//                for (int j = 0; j < 34; j++) {
//                    double t = Double.valueOf(jedis.lpop("vector"));
//                    temp[j] = t;
//                    jedis.rpush("vector", String.valueOf(t));
//                }
//                v.add(temp);
//            }
//
//            //jedis.rpush("vector", jedis.lpop("vector"));
//            for (int i = 0; i < 5; i++) {
//                for (int j = 0; j < 34; j++) {
//                    System.out.print(v.get(i)[j] + ' ');
//                }
//                System.out.println();
//            }
//            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//        }
    }
    @Test
    public void get() {
        Jedis jedis = TestRedis.getJedis();
        for (int i=0; i<13;i++) {
            for (int j=0; j<34; j++) {
                String t = jedis.lpop("vector");
                jedis.rpush("vector",t);
                t += "\n";
                System.out.println(t);
                TestWRInputFileForRedis.appendFile("/home/tkl/vector",t.getBytes(),1);
            }
        }
    }

    public static List<double[]> getDefineVectors(){
        Jedis jedis = TestRedis.getJedis();
        List<double[]> v = new ArrayList<>();
        for (int i = 0; i < 13; i++) {
            double[] temp = new double[34];
            for (int j = 0; j < 34; j++) {
                double t = Double.valueOf(jedis.lpop("vector"));
                temp[j] = t;
                jedis.rpush("vector", String.valueOf(t));
            }
            v.add(temp);
        }
        return v;
    }

    public static List<double[]> generateRandomVectors(int dimension, int vectorCount) {
        Random rand = new Random();
        return Stream.generate(() -> {
            double[] v = DoubleStream.generate(rand::nextGaussian).limit(dimension).toArray();
            double sum = Math.sqrt(Arrays.stream(v).map((d) -> d * d).sum());
            return Arrays.stream(v).map((d) -> d / sum).toArray();
        }).limit(vectorCount).collect(Collectors.toList());
    }

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
            System.out.println("origin storm");
        } else if (checktype == 1) {
            builder = new ResaTopologyBuilder();
            System.out.println("origin drs");
        } else if (checktype == 2) {
            builder = new SheddingResaTopologyBuilder();
            System.out.println("shedding drs");
        }
        int numWorkers = ConfigUtil.getInt(conf, "a-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a-acker.count", 1);
        System.out.println("worker number："+numWorkers);
        resaConfig.setNumWorkers(numWorkers);
        resaConfig.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");

        int defaultTaskNum = ConfigUtil.getInt(conf, "a-task.default", 1);
        //set spout
        int objectCount = ConfigUtil.getIntThrow(conf, "a-spout.object.size");
        builder.setSpout("objectSpout2",
                new ObjectSpoutSleep(host, port, queue, objectCount),
                ConfigUtil.getInt(conf, "a-spout.parallelism", 1));

        List<double[]> randVectors = getDefineVectors();//generateRandomVectors(ConfigUtil.getIntThrow(conf, "a-projection.dimension"),
                //ConfigUtil.getIntThrow(conf, "a-projection.size"));

        double projection_mu = ConfigUtil.getDouble(conf, "test.projection.mu", 1.0);
        double detector_mu = ConfigUtil.getDouble(conf, "test.detector.mu", 1.0);
        double updater_mu = ConfigUtil.getDouble(conf, "test.updater.mu", 1.0);

        builder.setBolt("projection",
                new ProjectionSleep(new ArrayList<>(randVectors), () -> (long) (-Math.log(Math.random()) * 1000.0 / projection_mu)), ConfigUtil.getInt(conf, "a-projection.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("objectSpout2");

        int minNeighborCount = ConfigUtil.getIntThrow(conf, "a-detector.neighbor.count.min");
        double maxNeighborDistance = ConfigUtil.getDoubleThrow(conf, "a-detector.neighbor.distance.max");
        builder.setBolt("detector",
                new DetectorSleep(objectCount, minNeighborCount, maxNeighborDistance, () -> (long) (-Math.log(Math.random()) * 1000.0 / detector_mu)),
                ConfigUtil.getInt(conf, "a-detector.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("projection", new Fields(ProjectionSleep.PROJECTION_ID_FIELD));

        builder.setBolt("updater",
                new UpdaterSleep(randVectors.size(),() -> (long) (-Math.log(Math.random()) * 1000.0 / updater_mu)), ConfigUtil.getInt(conf, "a-updater.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("detector", new Fields(ObjectSpoutSleep.TIME_FILED, ObjectSpoutSleep.ID_FILED));

        if (ConfigUtil.getBoolean(conf, "a-metric.resa", true)) {
            if (checktype == 1) {
                resaConfig.addDrsSupport();
                resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
                System.out.println("ResaMetricsCollector is registered");
            } else if (checktype == 2) {
                resaConfig.addSheddingSupport();
                resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
                System.out.println("shedding ResaMetricsCollector is registered");
            }
        }
        resaConfig.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));
//        if (ConfigUtil.getBoolean(conf, "a-metric.redis", true)) {
//            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
//            System.out.println("RedisMetricsCollector is registered");
//        }
        //resaConfig.setDebug(true);
       //LocalCluster localCluster  = new LocalCluster();
        //localCluster.submitTopology("111", resaConfig, builder.createTopology());
        //Utils.sleep(1000000000);
        TestRedis.add("type", "od");
        TestRedis.add("rebalance","0");
        TestRedis.add("time", String.valueOf(0));
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }

}
