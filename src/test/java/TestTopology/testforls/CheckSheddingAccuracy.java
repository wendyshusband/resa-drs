package TestTopology.testforls;

import java.util.*;

/**
 * Created by 44931 on 2017/8/15.
 */
public class CheckSheddingAccuracy {

    public static void main(String[] args) {
        CheckSheddingAccuracy.check();
    }
    private static int sizeOfBitSet = 1000;
    private static int fullBeginLine = 6938;//4288;
    private static int fullEndLine = 10000;//4298;
    private static int shedBeginLine = 1555;//2656;
    private static int shedEndLine = 5338;//2666;


    private static void checkFP() {
        List fulldata = TestWRInputFileForRedis
                .readFileByLine("E:/outlierdetection/fp/accuracy/dagan/daganbench", 100000);
                //.subList(fullBeginLine,fullEndLine);
        List sheddata = TestWRInputFileForRedis
                .readFileByLine("E:/outlierdetection/fp/accuracy/dagan/daganfp20", 100000);
                //.subList(shedBeginLine,shedEndLine);
        Iterator iteratorShed = sheddata.iterator();
        Iterator iteratorFull = fulldata.iterator();
        HashMap<String, Integer> map = new HashMap<>();
        while (iteratorFull.hasNext()) {
            String line = (String) iteratorFull.next();
            int temp = 1;
            if (map.containsKey(line)) {
                temp += map.get(line);
            }
            map.put(line, temp);
        }
        int same = 0;
        int fullTotal = fulldata.size();
        int shedTotal = sheddata.size();

        while (iteratorShed.hasNext()) {
            String line = (String) iteratorShed.next();
            if (map.containsKey(line)) {
                int count = map.get(line);
                if (count > 0) {
                    same++;
                    map.put(line, count - 1);
                }
            }
        }
        double precision = same * 1.0 / shedTotal;
        double recall = same * 1.0 / fullTotal;
        double f1 = (2.0 * recall * precision) / (recall + precision);
        System.out.println("same:"+same+" full:"+fullTotal+" shed:"+shedTotal);
        System.out.println("f1:"+f1);
        System.out.println("precision:"+precision);
        System.out.println("recall:"+recall);


    }
    private static void check() {
        List fulldata = TestWRInputFileForRedis
                .readFileByLine("E:/outlierdetection/fp/accuracy/real/newb201", 100000);
                //.subList(fullBeginLine,fullEndLine);
        List sheddata = TestWRInputFileForRedis
                .readFileByLine("E:/outlierdetection/fp/accuracy/real/new201", 100000);
                //.subList(shedBeginLine,shedEndLine);
        Iterator iteratorShed = sheddata.iterator();
        Iterator iteratorFull = fulldata.iterator();
        Map<Double,Integer> f1Result = new HashMap<>();
        Map<Double,Integer> precisionResult = new HashMap<>();
        Map<Double,Integer> recallResult = new HashMap<>();
        int miss;   int failure;    int right;  String[] result;  double precision;
        double recall;  double f1;  int temp;
        int count = 0;
        while (iteratorFull.hasNext() && iteratorShed.hasNext()) {
            count++;
            String[] fullChars = fix((String) iteratorFull.next());
            String[] shedChars = fix((String) iteratorShed.next());
            //System.out.println(shedChars.length+"|"+shedChars[0]);
            if (fullChars.length == 1 && fullChars[0].isEmpty()) {
                //fullChars = new String[]{String.valueOf(Integer.MAX_VALUE)};
                fullChars = new String[]{String.valueOf(sizeOfBitSet+1)};
            }
            if (shedChars.length == 1 && shedChars[0].isEmpty()) {
               // shedChars = new String[]{String.valueOf(Integer.MAX_VALUE)};
                shedChars = new String[]{String.valueOf(sizeOfBitSet+1)};
            }

            BitSet sFull = new BitSet(sizeOfBitSet);
            BitSet sShed = new BitSet(sizeOfBitSet);

            Arrays.stream(Arrays.stream(fullChars).mapToInt(Integer::valueOf).toArray()).forEach(e -> sFull.set(e, true));
            Arrays.stream(Arrays.stream(shedChars).mapToInt(Integer::valueOf).toArray()).forEach(e -> sShed.set(e, true));
            //System.out.println("f: "+sFull.toString());
            //System.out.println("s: "+sShed.toString());
            if (fullChars.length >= shedChars.length) {
                sFull.and(sShed);
                result = fix(sFull.toString());
            } else {
                sShed.and(sFull);
                result = fix(sShed.toString());
            }

            right = result.length;
            //System.out.println(Arrays.toString(result)+"right"+right);
            failure = shedChars.length - result.length;
            miss = fullChars.length - result.length;
            precision = Double.valueOf(String.format("%.2f",(right * 1.0 / shedChars.length)));
            recall = Double.valueOf(String.format("%.2f",(right * 1.0 / fullChars.length)));
            if (recall + precision == 0) {
                f1 = 0.0;
            } else {
                f1 = Double.valueOf(String.format("%.2f", ((2.0 * recall * precision) / (recall + precision))));
            }
            temp = f1Result.containsKey(f1) ? (f1Result.get(f1) + 1) : 1;
            f1Result.put(f1,temp);
            temp = precisionResult.containsKey(precision) ? (precisionResult.get(precision) + 1) : 1;
            precisionResult.put(precision,temp);
            temp = recallResult.containsKey(recall) ? (recallResult.get(recall) + 1) : 1;
            recallResult.put(recall,temp);
            //System.out.println(count);
        }

        //System.out.println(f1Result);
        System.out.println(f1Result.values());
        System.out.println(f1Result.keySet());
        int total = f1Result.values().stream().mapToInt(Number::intValue).sum();
        System.out.println("total: "+total);

        double finalResult = 0.0;
        for (Map.Entry entry : f1Result.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            finalResult += ((Double) entry.getKey() * t);
        }
        finalResult /= total;
        System.out.println("f1 = "+finalResult);
        double pr = 0.0;
        for (Map.Entry entry : precisionResult.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            pr += ((Double) entry.getKey() * t);
        }
        pr /= total;
        System.out.println("precision = "+pr);
        double r = 0.0;
        for (Map.Entry entry : recallResult.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            r += ((Double) entry.getKey() * t);
        }
        r /= total;
        System.out.println("recall = "+r);
    }

    private static String[] fix(String t1) {
        t1 = t1.replaceAll(" ", "");
        t1 = t1.replaceAll("\\{", "");
        t1 = t1.replaceAll("}", "");
        t1 = t1.replaceAll("\r", "");
        return t1.split(",");
    }
}
