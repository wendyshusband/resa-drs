package TestTopology.testforls;

import org.junit.Test;

import java.util.*;

/**
 * Created by 44931 on 2017/8/15.
 */
public class checkSheddingAccuracy {

    public static void main(String[] args) {
        checkSheddingAccuracy.check();
        checkSheddingAccuracy s = new checkSheddingAccuracy();
    }
    private static int sizeOfBitSet = 3000;
    private static int fullBeginLine = 0;//4288;
    private static int fullEndLine = 12000;//4298;
    private static int shedBeginLine = 0;//2656;
    private static int shedEndLine = 4800;//2666;
    @Test
    public void t() {
        //String s = "{0, 1, 2, 3, 8, 35, 36, 37, 38, 175, 176, 177, 178, 229, 230, 233, 234, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262}";
        String v = "{0,1,2\r}";
        //s = s.replaceAll(" ", "");
        //s = s.replaceAll("\\{", "");
        //s = s.replaceAll("}", "");
        v = v.replaceAll(" ", "");
        v = v.replaceAll("\\{", "");
        v = v.replaceAll("}", "");
        System.out.println(v);
        String[] ss = v.split(",");
        System.out.println(ss.length);
        //int[] intfulls= Arrays.asList(ss).stream().mapToInt(Integer::valueOf).toArray();
        //System.out.println(intfulls[2]);
        System.out.println(ss[ss.length-1]+"@@@"+ss[ss.length-1]+"++++++"+ss[ss.length-1]);
        System.out.println("1234567890"+"\r"+"abc");
        List<String> a = new ArrayList<>();
        a.add("111");
        a.add("222");
        a.add("333");
        a.add("444");
        a.add("555");
        a.subList(1,3).stream().forEach(e -> System.out.println(e));

    }
    private static void check() {
        List fulldata = TestWRInputFileForRedis
                .readFileByLine("E:/outlierdetection/3test30001/detector/0.0/full.txt", 100000)
                .subList(fullBeginLine,fullEndLine);
        List sheddata = TestWRInputFileForRedis
                .readFileByLine("E:/outlierdetection/3test30001/spout/0.9/full.txt", 100000);
                //.subList(shedBeginLine,shedEndLine);
        Iterator iteratorShed = sheddata.iterator();
        Iterator iteratorFull = fulldata.iterator();
        Map<Double,Integer> f1Result = new HashMap<>();
        Map<Double,Integer> precisionResult = new HashMap<>();
        Map<Double,Integer> recallResult = new HashMap<>();
        int miss;
        int failure;
        int right;
        String[] result;
        double precision;
        double recall;
        double f1;
        int temp;
        int count = 0;
        while (iteratorFull.hasNext() && iteratorShed.hasNext()) {
            count++;
            String[] fullChars = fix((String) iteratorFull.next());
            String[] shedChars = fix((String) iteratorShed.next());
            //System.out.println(shedChars.length+"|"+shedChars[0]);
            if (fullChars.length == 1 && fullChars[0].isEmpty()) {
                fullChars = new String[]{String.valueOf(Integer.MAX_VALUE)};
            }
            if (shedChars.length == 1 && shedChars[0].isEmpty()) {
                shedChars = new String[]{String.valueOf(Integer.MAX_VALUE)};
            }

            BitSet sFull = new BitSet(sizeOfBitSet);
            BitSet sShed = new BitSet(sizeOfBitSet);

            Arrays.stream(Arrays.stream(fullChars).mapToInt(Integer::valueOf).toArray()).forEach(e -> sFull.set(e, true));
            Arrays.stream(Arrays.stream(shedChars).mapToInt(Integer::valueOf).toArray()).forEach(e -> sShed.set(e, true));
            if (fullChars.length >= shedChars.length) {
                sFull.and(sShed);
                result = fix(sFull.toString());
            } else {
                sShed.and(sFull);
                result = fix(sShed.toString());
            }

            right = result.length;
            failure = shedChars.length - result.length;
            miss = fullChars.length - result.length;
            precision = Double.valueOf(String.format("%.2f",(right * 1.0 / shedChars.length)));
            recall = Double.valueOf(String.format("%.2f",(right * 1.0 / fullChars.length)));
            if (recall + precision == 0) {
                f1 = 0.0;
            } else {
                f1 = Double.valueOf(String.format("%.2f", ((2.0 * recall * precision) / (recall + precision))));
            }
            //System.out.println(f1+"~~~"+precision+"~"+recall+"~"+right);
            temp = f1Result.containsKey(f1) ? (f1Result.get(f1) + 1) : 1;
            f1Result.put(f1,temp);
            temp = precisionResult.containsKey(precision) ? (precisionResult.get(precision) + 1) : 1;
            precisionResult.put(precision,temp);
            temp = recallResult.containsKey(recall) ? (recallResult.get(recall) + 1) : 1;
            recallResult.put(recall,temp);
            //System.out.println(f1);
            //System.out.println("result " + result.length + " fullsize:"+fullChars.length+" shedsize:"+shedChars.length);
            //System.out.println("fail:"+failure+" miss:"+miss+" precision:" + precision + " recall:" + recall + " f1:" + f1);
        }
        System.out.println(f1Result);
        System.out.println(f1Result.values());
        System.out.println(f1Result.keySet());
        int total = f1Result.values().stream().mapToInt(Number::intValue).sum();
        System.out.println("total: "+total);
        //f1Result.keySet().stream().forEach(e -> System.out.println(e));
        //System.out.println();
        //f1Result.values().stream().forEach(e -> System.out.println(e));
        double finalResult = 0.0;
        for (Map.Entry entry : f1Result.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            finalResult += ((Double) entry.getKey() * t) / total;
        }
        System.out.println(finalResult);
        //System.out.println(precisionResult);
        //System.out.println(recallResult);
    }

    private static String[] fix(String t1) {
        t1 = t1.replaceAll(" ", "");
        t1 = t1.replaceAll("\\{", "");
        t1 = t1.replaceAll("}", "");
        t1 = t1.replaceAll("\r", "");
        return t1.split(",");
    }
}
