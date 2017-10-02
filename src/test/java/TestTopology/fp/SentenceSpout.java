package TestTopology.fp;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import resa.shedding.tools.TestRedis;
import resa.topology.RedisQueueSpout;


/**
 * Created by ding on 14-6-5.
 */
public class SentenceSpout extends RedisQueueSpout implements Constant {
    public SentenceSpout(String host, int port, String queue) {
        super(host, port, queue);
    }
    private static Jedis jedis = TestRedis.getJedis();

    @Override
    protected void emitData(Object data) {
        int a;
        String s = jedis.get("time");
        a = Integer.valueOf(s);
        a++;
        try {
            TestRedis.add("time", String.valueOf(a));
        } catch (Exception e) {
            e.printStackTrace();
        }
        String text = (String) data;
        collector.emit(new Values(((String) data).substring(1), text.charAt(0) == '+'), "");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SENTENCE_FIELD, IS_ADD_FIELD));
    }
}
