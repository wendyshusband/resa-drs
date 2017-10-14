package TestTopology.outdetsleep;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import resa.shedding.tools.TestRedis;
import resa.topology.RedisQueueSpout;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created by kailin on 14-3-14.
 */
public class ObjectSpoutSleep extends RedisQueueSpout {
    public static final String JB_FILED = "jb";
    public static final String ID_FILED = "id";
    public static final String VECTOR_FILED = "vector";
    public static final String TIME_FILED = "time";
    private final int objectCount;
    private static Jedis jedis = TestRedis.getJedis();

    public ObjectSpoutSleep(String host, int port, String queue, int objectCount) {
        super(host, port, queue);
        this.objectCount = objectCount;
        System.out.println("objcOunt "+objectCount);
    }

    @Override
    protected void emitData(Object data) {
        String[] tmp = ((String) data).split("[|]");
        double[] v = Arrays.stream(tmp[2].split(",")).mapToDouble((str) -> Double.parseDouble(str)).toArray();
        Integer objId = (int) (Long.parseLong(tmp[0]) % objectCount);
        Long timestamp = Long.parseLong(tmp[1]);

        int a;
        String s = jedis.get("time");
        a = Integer.valueOf(s);
        a++;
        try {
            TestRedis.add("time", String.valueOf(a));
        } catch (Exception e) {
            e.printStackTrace();
        }
        collector.emit(new Values(a, objId, v, timestamp), Collections.singletonMap(objId, timestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(JB_FILED, ID_FILED, VECTOR_FILED, TIME_FILED));
    }
}

//