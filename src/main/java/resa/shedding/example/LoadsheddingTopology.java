package resa.shedding.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by kailin on 4/3/17.
 */
public class LoadsheddingTopology {
    public static class TestOverLoadSpout extends BaseRichSpout {
        public static Logger LOG = LoggerFactory.getLogger(TestOverLoadSpout.class);

        boolean _isDistributed;
        SpoutOutputCollector _collector;

        public TestOverLoadSpout() {
            this(true);
        }

        public TestOverLoadSpout(boolean isDistributed) {
            _isDistributed = isDistributed;
        }

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            _collector = spoutOutputCollector;
        }

        public void nextTuple() {
            Utils.sleep(100);
            final Random random = new Random();
            _collector.emit(new Values(String.valueOf(random.nextInt(1000))));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("resource"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if(!_isDistributed){
                Map<String, Object> ret = new HashMap<String, Object>();
                ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
                return ret;
            }else{
                return null;
            }

        }
    }

    public static class WorkBolt implements IRichBolt {
        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            _collector.emit(tuple,new Values(tuple.getValue(0)+"tail"));
            _collector.ack(tuple);
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }


        public static void main(String[] args)  {
            File f = new File("/usr/sss");
            System.out.println(f.mkdirs());
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestOverLoadSpout(false), 3);
        builder.setBolt("loadshedding", new RandomSheddableBolt(new WorkBolt(),new ExampleShedder()), 2).shuffleGrouping("spout");
        builder.setBolt("output",new outputBolt(),1).shuffleGrouping("loadshedding");
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
        }
    }
}
