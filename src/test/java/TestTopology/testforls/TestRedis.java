package TestTopology.testforls;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by kailin on 2016/12/28.
 */
public class TestRedis {
    private static JedisPool jedisPool = null;
    static{
        int maxActivity = 50;
        int maxIdle = 10;
        long maxWait = 3000;
        boolean testOnBorrow = true;
        boolean onreturn = true;

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxActivity);
        config.setMaxIdle(maxIdle);
        config.setMaxWaitMillis(maxWait);
        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnReturn(onreturn);
        jedisPool = new JedisPool(config, "10.21.50.20", 6379);
    }

    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * release jedis
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * search
     */
    public String  find(String key,String value){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * search special string
     */
    public String findSubStr(String key,Integer startOffset,Integer endOffset){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
            jedisPool.returnResource(jedis);
        }
    }
    /**
     * add
     * @param key key
     * @param value value
     * @return
     * @throws Exception
     */
    public static int add(String key,String value) throws Exception{
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.set(key, value);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }finally{
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * delete
     * @param key
     * @return
     */
    public static int del(String key){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.del(key);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }finally{
            jedisPool.returnResource(jedis);
        }
    }

    public static void main(String[] args) {
        TestRedis rea = new TestRedis();
        Jedis jedis = rea.getJedis();
        String[] s = {
                "i am a student.",
                //"you are a player.",
                //"i like you.",
                //"this salad is very good."
        };
//        int i = 50000;
//        while(i>0){
//            for(int j=0;j<s.length;j++){
//                jedis.lpush("fsource",s[j]);
//            }
//            i--;
//        }
        int i = 0;
        while(i<200000){
            i++;
            //jedis.lpop("outputMetric");
            if(null == jedis.lpop("fsource")){
                break;
            }
        }
        System.out.println("ok!");
    }
}

