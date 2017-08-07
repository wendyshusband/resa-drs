package TestTopology.helper;

import org.apache.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import resa.util.ConfigUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.LongSupplier;

/**
 * Created by ding on 14-3-18.
 */
public class DataSender {

    private static final String END = new String("end");

    private String host;
    private int port;
    private String queueName;

    public DataSender(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = (String) conf.get("redis.queue");
    }

    private class PushThread extends Thread {

        private BlockingQueue<String> dataQueue;
        private Jedis jedis = new Jedis(host, port);

        private PushThread(BlockingQueue<String> dataQueue) {
            this.dataQueue = dataQueue;
        }

        @Override
        public void run() {
            String line;
            try {
                while ((line = dataQueue.take()) != END) {
                    jedis.rpush(queueName, line);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                dataQueue.offer(END);
            }
        }
    }
    public void send2Queue(Path inputFile, int batchSize, LongSupplier sleep) throws IOException, InterruptedException {
        BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(10000);
        for (int i = 0; i < 1; i++) {
            new PushThread(dataQueue).start();
        }
        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line;
            int batchCnt = 0;
            while ((line = reader.readLine()) != null) {
                dataQueue.put(processData(line));
                count++;
                if (++batchCnt == batchSize) {
                    batchCnt = 0;
                    long ms = sleep.getAsLong();
                    if (ms > 0) {
                        System.out.println(count+"always ms"+ms);
                        Utils.sleep(ms);
                    }
                }
            }
        } finally {
            dataQueue.put(END);
        }
    }

    public void send2QueueControlTime(Path inputFile, int batchSize, LongSupplier sleep) throws IOException, InterruptedException {
        BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(10000);
        for (int i = 0; i < 1; i++) {
            new PushThread(dataQueue).start();
        }
        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line;
            int batchCnt = 0;
            long time = System.currentTimeMillis();
            while ((line = reader.readLine()) != null && (System.currentTimeMillis() - time) <= (200 * 1000)) {//tkl
                dataQueue.put(processData(line));
                count++;
                if (++batchCnt == batchSize) {
                    batchCnt = 0;
                    long ms = sleep.getAsLong();
                    if (ms > 0) {
                        System.out.println("ms"+ms);
                        Utils.sleep(ms);
                    }
                }
            }
        } finally {
            dataQueue.put(END);
            System.out.println("count= "+count);
        }
    }

    public void send2QueueForOutLier(Path inputFile, int batchSize, LongSupplier sleep) throws IOException, InterruptedException {
        BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(10000);
        for (int i = 0; i < 1; i++) {
            new PushThread(dataQueue).start();
        }
        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line;
            int batchCnt = 0;
            long time = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {// && (System.currentTimeMillis() - time) <= (200 * 1000)) {//tkl
                dataQueue.put(fixAndProcessData(count, line));
                count++;
                if (++batchCnt == batchSize) {
                    batchCnt = 0;
                    long ms = sleep.getAsLong();
                    if (ms > 0) {
                        Utils.sleep(ms);
                    }
                }
            }
        } finally {
            dataQueue.put(END);
            System.out.println("count= "+count);
        }
    }

    private String fixAndProcessData(int count, String line) {
        long time = System.currentTimeMillis();
        return count+"|"+time+"|"+line;
    }


    protected String processData(String line) {
        return line;
    }


    private static void printUsage() {
        System.out.println("usage: DataSender <confFile> <inputFile> <batchSize>" +
                " [-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
    }

    protected static void runWithInstance(DataSender sender, String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            printUsage();
            System.out.println(1);
            return;
        }
        int batchSize = Integer.parseInt(args[2]);
        System.out.println("start sender" + batchSize);
        Path dataFile = Paths.get(args[1]);
        switch (args[3].substring(1)) {
            case "deter":
                System.out.println("case deter");
                long sleep = (long) (1000 / Float.parseFloat(args[4]));
                sender.send2Queue(dataFile, batchSize, () -> sleep);
                break;
            case "poison":
                double lambda = Float.parseFloat(args[4]);
                System.out.println("case poison "+lambda);
                if (lambda >=12) {
                    sender.send2Queue(dataFile, batchSize, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
                } else {
                    sender.send2QueueControlTime(dataFile, batchSize, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
                }
                break;
            case "uniform":
                System.out.println("case uniform");
                if (args.length < 6) {
                    printUsage();
                    System.out.println(2);
                    return;
                }
                double left = Float.parseFloat(args[4]);
                double right = Float.parseFloat(args[5]);
                sender.send2Queue(dataFile, batchSize, () -> (long) (1000 / (Math.random() * (right - left) + left)));
                break;
            default:
                printUsage();
                System.out.println(3);
                break;
        }
        System.out.println("end sender");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String[] arg = new String[5];
        arg[0] = "";
        //arg[1] = "E:/testData/testRedis.txt";
        arg[1] = "";
        arg[2] = String.valueOf(1);
        arg[3] = " poison";
        int load = 2;
        long startTime = System.currentTimeMillis();
        while (load <= 12) {
            load +=2;
            arg[4] = String.valueOf(load);
            DataSender sender = new DataSender(ConfigUtil.readConfig(new File(args[0])));
            runWithInstance(sender, arg);
        }
    }
}
