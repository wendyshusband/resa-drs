package TestTopology.TestPassiveShedding;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.shedding.tools.FrequencyRestrictor;
import resa.util.ConfigUtil;

import java.util.Map;
import java.util.UUID;

public class ZeroSpout extends BaseRichSpout {
  private static final Logger LOG = LoggerFactory.getLogger(ZeroSpout.class);
  SpoutOutputCollector _collector;
  private FrequencyRestrictor frequencyRestrictor;
  private static int co = 0;
  private int number;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

    _collector = collector;
    frequencyRestrictor = new FrequencyRestrictor(ConfigUtil.getInt(conf, "maxFrequencyPerSecond", 500),
            ConfigUtil.getInt(conf, "windowsPerSecond", 500));
    number = ConfigUtil.getInt(conf, "wc-number", 10000);
  }

  @Override
  public void nextTuple() {
    if (frequencyRestrictor.tryPermission() && co < number) {
      co++;
      _collector.emit(new Values(0), UUID.randomUUID().toString());
    }
  }

  @Override
  public void ack(Object id) {

  }

  @Override
  public void fail(Object id) {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("zero"));
  }

}
