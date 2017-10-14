package resa.shedding.tools;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by kailin on 2016/12/28.
 */
public class TestRedis {
    private static JedisPool jedisPool = null;
    private static InetAddress addr;
    static{
        int maxActivity = 100;
        int maxIdle = 50;
        long maxWait = 30000;
        boolean testOnBorrow = true;
        boolean onreturn = true;

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxActivity);
        config.setMaxIdle(maxIdle);
        config.setMaxWaitMillis(maxWait);
        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnReturn(onreturn);
        try {
            addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        //jedisPool = new JedisPool(config, addr.getHostAddress(), 6379,100000000);
        jedisPool = new JedisPool(config, "10.21.50.43", 6379,100000000);
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

    public static void insertList(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.lpush(key,value);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            jedisPool.returnResource(jedis);
        }
    }
    
    public static void main(String[] args) {
        System.out.println("aaa");
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

//        int i = 0;
//        while(i<200000){
//            i++;
//            System.out.println(jedis.lpop("fsource"));
//            if(null == jedis.lpop("fsource")){
//                //System.out.println("null");
//                //break;
//            }
//        }
//        System.out.println("ok!");
        int i = 1;
        try {
            TestRedis.add("time", String.valueOf(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(i<1000) {
            int a;
            String ss = jedis.get("time");

            a = Integer.valueOf(ss);
            a++;
            System.out.println(ss+" a: "+a);
            try {
                TestRedis.add("time", String.valueOf(a));
            } catch (Exception e) {
                e.printStackTrace();
            }
            i++;
        }
    }
}

