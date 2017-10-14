package TestTopology.helper;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import redis.clients.jedis.Jedis;
import resa.shedding.tools.FrequencyRestrictor;
import resa.util.ConfigUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kailin.
 */
public class RedisQueueSpout2 extends BaseRichSpout {

    public static final String OUTPUT_FIELD_NAME = "text";
    protected SpoutOutputCollector collector;

    private String queue;
    private String host;
    private byte[] byteQueueName;
    private int port;
    private transient Jedis jedis = null;
    private FrequencyRestrictor frequencyRestrictor;
    private static AtomicInteger co = new AtomicInteger(0);
    private int number;

    public RedisQueueSpout2(String host, int port, String queue) {
        this.host = host;
        this.port = port;
        this.queue = queue;
    }

    public RedisQueueSpout2(String host, int port, String queue, boolean useBinary) {
        this.host = host;
        this.port = port;
        this.queue = queue;
        useBinary(useBinary);
    }

    public void useBinary(boolean use) {
        if (use) {
            byteQueueName = queue.getBytes();
        } else {
            byteQueueName = null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(OUTPUT_FIELD_NAME));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        frequencyRestrictor = new FrequencyRestrictor(ConfigUtil.getInt(map, "maxFrequencyPerSecond", 500),
                ConfigUtil.getInt(map, "windowsPerSecond", 500));
        number = ConfigUtil.getInt(map, "word-number", 10000);
    }

    @Override
    public void close() {
        disconnect();
    }

    @Override
    public void nextTuple() {
        if (frequencyRestrictor.tryPermission() && co.get() < number) {
            co.getAndIncrement();
            Jedis jedis = getConnectedJedis();
            if (jedis == null) {
                System.out.println("FrameSourceFox.Prepare, jedis == null");
                return;
            }
            Object text;
            try {
                text = byteQueueName == null ? jedis.lpop(queue) : jedis.lpop(byteQueueName);
            } catch (Exception e) {
                disconnect();
                return;
            }
            if (text != null) {
                emitData(text);
            }
        }
    }

    protected void emitData(Object data) {
        collector.emit(Arrays.asList(data), data);
    }

    private Jedis getConnectedJedis() {
        if (jedis != null) {
            return jedis;
        }
        //try connect to redis server
        try {
            jedis = new Jedis(host, port);
        } catch (Exception e) {
        }
        return jedis;
    }

    private void disconnect() {
        try {
            jedis.disconnect();
        } catch (Exception e) {
        }
        jedis = null;
    }

}
