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
 * Created by kailin on 17-8-18.
 */
public class NewDataSender {

    private static final String END = new String("end");
    private static final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(20010);
    private String host;
    private int port;
    private String queueName;


    public NewDataSender(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = (String) conf.get("redis.queue");
    }

    private class PushThread extends Thread {

        private BlockingQueue<String> dataQueue;
        private Jedis jedis = new Jedis(host, port,1000000);

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
        for (int i = 0; i < 3; i++) {
            new PushThread(dataQueue).start();
        }
        int count = 0;
        System.out.println(inputFile.toAbsolutePath());
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {

            String line;
            int batchCnt = 0;
            while ((line = reader.readLine()) != null) {//!queue.isEmpty()
                //line = queue.poll();
                //String insert = fixAndProcessData((count % 700),line);
                String insert = processData(line);
                dataQueue.put(insert);
                count++;
                if (++batchCnt == batchSize) {
                    batchCnt = 0;
                    long ms = sleep.getAsLong();
                    if (ms > 0) {
                        System.out.println(count+" always:" + insert);
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
            while ((System.currentTimeMillis() - time) <= (200 * 1000)) {//tkl (line = reader.readLine()) != null &&
                line = queue.poll();
                //dataQueue.put(processData(line));
                String insert = fixAndProcessData((count % 700),line);
                dataQueue.put(insert);
                count++;
                if (++batchCnt == batchSize) {
                    batchCnt = 0;
                    long ms = sleep.getAsLong();
                    if (ms > 0) {
                        System.out.println("data: "+insert);
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
        BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(7000);
        for (int i = 0; i < 1; i++) {
            new PushThread(dataQueue).start();
        }
        int count = 0;
        try {
            String line;
            int batchCnt = 0;
            while (count < 7000) {//7000 && (System.currentTimeMillis() - time) <= (200 * 1000)) {//tkl
                line = queue.poll();
                if (line != null) {
                    String insert = fixAndProcessData((count % 700),line);
                    System.out.println(insert);
                    dataQueue.put(insert);
                    count++;
                    if (++batchCnt == batchSize) {
                        batchCnt = 0;
                        long ms = sleep.getAsLong();
                        if (ms > 0) {
                            Utils.sleep(ms);
                        }
                    }
                } else {
                    break;
                }
            }
        } finally {
            dataQueue.put(END);
            System.err.println("count= "+count);
            System.err.println("queueSize= "+queue.size());
        }
    }

    private String fixAndProcessData(int count, String line) {
        long time = System.currentTimeMillis();
        String result = count+"|"+time+"|"+line;
        //System.out.println(result);
        return result;
    }


    protected String processData(String line) {
        //System.out.println(line);
        return line;
    }


    private static void printUsage() {
        System.out.println("usage: DataSender <confFile> <inputFile> <batchSize>" +
                " [-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
    }

    protected static void runWithInstance(NewDataSender sender, String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            printUsage();
            return;
        }
        int batchSize = Integer.parseInt(args[2]);
        System.out.println("start sender");
        Path dataFile = Paths.get(args[1]);
        switch (args[3].substring(1)) {
            case "deter":
                long sleep = (long) (1000 / Float.parseFloat(args[4]));
                sender.send2Queue(dataFile, batchSize, () -> sleep);
                break;
            case "poison":
                double lambda = Float.parseFloat(args[4]);
                System.out.println("case poison "+lambda);
                sender.send2QueueForOutLier(dataFile, batchSize, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
//                if (lambda >=12) {
//                    sender.send2Queue(dataFile, batchSize, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
//                } else {
//                    sender.send2QueueControlTime(dataFile, batchSize, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
                //}
                break;
            case "uniform":
                if (args.length < 6) {
                    printUsage();
                    return;
                }
                double left = Float.parseFloat(args[4]);
                double right = Float.parseFloat(args[5]);
                sender.send2Queue(dataFile, batchSize, () -> (long) (1000 / (Math.random() * (right - left) + left)));
                break;
            default:
                printUsage();
                break;
        }
        System.out.println("end sender");
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        sendToQ();
        String outFile = "E:/outlierdetection/kddcup.data.11000.head";
        String fpFile = "E:/outlierdetection/topk/words1000.txt";
        String[] arg = new String[5];
        arg[0] = "";
        arg[1] = fpFile;
        arg[2] = String.valueOf(1);
        arg[3] = " poison";
        int load = 20;
        int count = 0;
        long startTime = System.currentTimeMillis();
//        while (load <= 1120 && load >= 1) {
//            if (count < 4) {
//                  load += 0;
//                count++;
//            } else if (count < 7) {
//                load -=2;
//                count++;
//            } else break;
        arg[4] = String.valueOf(load);
        NewDataSender sender = new NewDataSender(ConfigUtil.readConfig(new File(args[0])));
        runWithInstance(sender, arg);
        System.out.println(System.currentTimeMillis() - startTime);
        System.out.println(1000*1000/(System.currentTimeMillis() - startTime));
//       }
    }

    static void stableSend() {}
    static void loadChangeSend() {}

    public static void sendToQ () {
        long start = System.currentTimeMillis();
        Path path = Paths.get("E:/outlierdetection/kddcup.data.11000.head");
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            int a = 7000;//7000;
            while ((line = reader.readLine()) != null && a>=1) {// && (System.currentTimeMillis() - time) <= (200 * 1000)) {//tkl
                queue.put(line);
                a--;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("queuesize"+queue.size());
            System.out.println("times"+(System.currentTimeMillis() - start));
        }
    }
}
