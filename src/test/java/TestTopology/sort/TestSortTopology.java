package TestTopology.sort;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import resa.shedding.example.outputBolt;
import resa.util.ConfigUtil;

import java.io.File;


/**
 * Created by kailin on 11/4/17.
 */
public class TestSortTopology {
    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        TopologyBuilder builder = new TopologyBuilder();
        //int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 10);

        builder.setSpout("sort-SpoutA", new SortSpout(true,"A"),
                ConfigUtil.getInt(conf, "sort-spoutA-parallelism", 1));

//        builder.setSpout("sort-SpoutE", new TestSortSpout(true,"E"),
//                ConfigUtil.getInt(conf, "sort-spoutE-parallelism", 1));

        builder.setBolt("sort-BoltB",new SortWorkBolt2Path("B"),
                ConfigUtil.getInt(conf, "sort-BoltB.parallelism", 1))
                .shuffleGrouping("sort-SpoutA");

        builder.setBolt("sort-BoltD",new SortWorkBolt("D"),
                ConfigUtil.getInt(conf, "sort-BoltD.parallelism", 1))
                //.setNumTasks(defaultTaskNum)
                .shuffleGrouping("sort-BoltB","D-Stream");

        builder.setBolt("sort-BoltC",new outputBolt(),//new SortWorkBolt("C"),
                ConfigUtil.getInt(conf, "sort-BoltC.parallelism", 1))
                //.setNumTasks(defaultTaskNum)
                //.shuffleGrouping("sort-SpoutE")
                .shuffleGrouping("sort-BoltB","C-Stream");

        builder.setBolt("sort-BoltF",new outputBolt(),
                ConfigUtil.getInt(conf, "sort-BoltF.parallelism", 1))
                .shuffleGrouping("sort-BoltC");

        conf.setNumWorkers(ConfigUtil.getInt(conf, "sort-NumOfWorkers", 1));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
