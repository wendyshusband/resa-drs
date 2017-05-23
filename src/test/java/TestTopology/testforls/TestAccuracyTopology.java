package TestTopology.testforls;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import resa.util.ConfigUtil;

import java.io.File;

/**
 * Created by kailin on 22/5/17.
 */
public class TestAccuracyTopology {
    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[0]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 10);

        builder.setSpout("TestAccuracySpout", new TestAccuracySpout(),
                ConfigUtil.getInt(conf, "TestAccuracySpout-parallelism", 1));

        builder.setBolt("TestAccuracyBolt",new TestAccuracyBolt(),
                ConfigUtil.getInt(conf, "TestAccuracyBolt.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("TestAccuracySpout");

        conf.setNumWorkers(ConfigUtil.getInt(conf, "TestAccuracy-NumOfWorkers", 1));
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "TestAccuracy-MaxSpoutPending", 0));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
    }
}
