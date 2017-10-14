package TestTopology.wc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * Created by ding on 15/1/6.
 */
public class ResaWordCount {

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[0]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new ResaTopologyBuilder();

        if (!ConfigUtil.getBoolean(conf, "spout.redis", false)) {
            builder.setSpout("say", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "spout.parallelism", 1));
        } else {
            String host = (String) conf.get("redis.host");
            int port = ((Number) conf.get("redis.port")).intValue();
            String queue = (String) conf.get("redis.queue");
            builder.setSpout("say", new RedisSentenceSpout(host, port, queue),
                    ConfigUtil.getInt(conf, "spout.parallelism", 1));
        }
        builder.setBolt("split", new WordCountTopology.SplitSentence(), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .shuffleGrouping("say");
        builder.setBolt("counter", new WordCountTopology.WordCount(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .fieldsGrouping("split", new Fields("word"));
        // add drs component
        //resaConfig.addDrsSupport();
        resaConfig.setNumWorkers(ConfigUtil.getInt(conf, "wc-NumOfWorkers", 1));
        resaConfig.setMaxSpoutPending(ConfigUtil.getInt(conf, "wc-MaxSpoutPending", 0));
        resaConfig.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        resaConfig.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        StormSubmitter.submitTopology(args[1], resaConfig, builder.createTopology());
    }

}
