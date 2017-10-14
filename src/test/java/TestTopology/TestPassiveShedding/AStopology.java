package TestTopology.TestPassiveShedding;

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
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class AStopology {

    public static class AddBolt extends TASleepBolt {
        OutputCollector _collector;
        public AddBolt(IntervalSupplier sleep){
            super(sleep);
            System.out.println("addleeptime:"+sleep.get());
        }

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            super.execute(tuple);
            int i = tuple.getIntegerByField("zero");
            _collector.emit(tuple,new Values(++i));
            _collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("add"));
        }
    }

    public static class SubBolt extends TASleepBolt {
        OutputCollector _collector;

        public SubBolt(IntervalSupplier sleep){
            super(sleep);
            System.out.println("subleeptime:"+sleep.get());
        }

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            super.execute(tuple);
            int i = tuple.getIntegerByField("add");
            _collector.emit(tuple,new Values(--i));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sub"));
        }
    }

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("zero", new ZeroSpout(),ConfigUtil.getInt(conf, "spout.parallelism", 1));
        double add_mu = ConfigUtil.getDouble(conf, "add.mu", 1.0);
        System.out.println("addmumumu:"+add_mu);
        builder.setBolt("add", new AddBolt(() -> (long) (1000.0 / add_mu)), ConfigUtil.getInt(conf, "add.parallelism", 1)).shuffleGrouping("zero");
        double sub_mu = ConfigUtil.getDouble(conf, "sub.mu", 1.0);
        System.out.println("submumumu:"+sub_mu);
        builder.setBolt("sub", new SubBolt(() -> (long) (1000.0 / sub_mu)), ConfigUtil.getInt(conf, "sub.parallelism", 1)).shuffleGrouping("add");
        builder.setBolt("out", new Output2(() -> (long) (1000.0 / sub_mu)), 1).shuffleGrouping("sub");

        //conf.setDebug(true);
        conf.setNumWorkers(ConfigUtil.getInt(conf, "wc-NumOfWorkers", 1));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
}
