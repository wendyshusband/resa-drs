package TestTopology.testforls;

import org.junit.Test;

import javax.print.DocFlavor;
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
                .readFileByLine("/opt/oddata/od20171018/odbench1", 100000);
                //.subList(fullBeginLine,fullEndLine);
        List sheddata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/1300/od1300shed161", 100000);
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
                .readFileByLine("/opt/oddata/20171023all/zuixin/juebugai/jbs3", 100000);
                //.subList(fullBeginLine,fullEndLine);
        List sheddata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/20171023all/zuixin/juebugai/jcs3", 100000);
                //.subList(shedBeginLine,shedEndLine)
        Iterator iteratorShed = sheddata.iterator();
        Iterator iteratorFull = fulldata.iterator();
        Map<Double,Integer> f1Result = new HashMap<>();
        Map<Double,Integer> precisionResult = new HashMap<>();
        Map<Double,Integer> recallResult = new HashMap<>();
        int miss = 0;   int failure = 0;
        int right;  String[] result;  double precision;
        double recall;  double f1;  int temp;
        int count = 0;
        HashMap<String,String[]> realmap = new HashMap<>();
        while (iteratorFull.hasNext()) {
            HashMap<String,String[]> fullmap = fix2((String) iteratorFull.next());
            for (String str : fullmap.keySet()) {
                if (!realmap.containsKey(str)) {
                    realmap.put(str,fullmap.get(str));
                }
            }
        }
        System.out.println("real map size:"+realmap.size());
        while (iteratorShed.hasNext()) {
            count++;
            HashMap<String,String[]> shedmap = fix2((String) iteratorShed.next());
            for (Map.Entry e : shedmap.entrySet()) {
                String shedkey = (String) e.getKey();
                String[] shedChars = (String[]) e.getValue();
                if (realmap.containsKey(shedkey)) {
                    String[] fullChars = realmap.get(shedkey);
                    if (fullChars.length == 1 && fullChars[0].isEmpty()) {
                        fullChars = new String[]{String.valueOf(sizeOfBitSet + 1)};
                    }
                    if (shedChars.length == 1 && shedChars[0].isEmpty()) {
                        shedChars = new String[]{String.valueOf(sizeOfBitSet + 1)};
                    }
                    BitSet sFull = new BitSet(sizeOfBitSet);
                    BitSet sShed = new BitSet(sizeOfBitSet);
                    Arrays.stream(Arrays.stream(fullChars).mapToInt(Integer::valueOf).toArray()).forEach(es -> sFull.set(es, true));
                    Arrays.stream(Arrays.stream(shedChars).mapToInt(Integer::valueOf).toArray()).forEach(es -> sShed.set(es, true));
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
                }
            }
        }
        System.out.println("count="+count+" failure="+failure+" miss="+miss);
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
        //finalResult /= total;
        finalResult /= realmap.size();
        System.out.println("f1 = "+finalResult);
        double pr = 0.0;
        for (Map.Entry entry : precisionResult.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            pr += ((Double) entry.getKey() * t);
        }
        //pr /= total;
        pr /= realmap.size();
        System.out.println("precision = "+pr);
        double r = 0.0;
        for (Map.Entry entry : recallResult.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            r += ((Double) entry.getKey() * t);
        }
        //r /= total;
        r /= realmap.size();
        System.out.println("recall = "+r);
    }

    private static HashMap<String,String[]> fix2(String t1) {
        String[] ss = t1.split(":");
        ss[1] = ss[1].replaceAll(" ", "");
        ss[1] = ss[1].replaceAll("\\{", "");
        ss[1] = ss[1].replaceAll("}", "");
        ss[1] = ss[1].replaceAll("\r", "");
        String[] res = ss[1].split(",");
        HashMap<String,String[]> map = new HashMap();
        map.put(ss[0],res);
        return map;
    }

    private static String[] fix(String t1) {
        t1 = t1.replaceAll(" ", "");
        t1 = t1.replaceAll("\\{", "");
        t1 = t1.replaceAll("}", "");
        t1 = t1.replaceAll("\r", "");
        return t1.split(",");
    }

    private static void checkAc() {
        List fulldata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/mostdiao/eaccbench1", 100000);
        //.subList(fullBeginLine,fullEndLine);
        List sheddata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/mostdiao/eaccupdater041", 100000);
        HashMap a1 = getAccuracy(fulldata,sheddata);
        fulldata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/mostdiao/eaccbench2", 100000);
        sheddata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/mostdiao/eaccupdater042", 100000);
        HashMap a2 = getAccuracy(fulldata,sheddata);
        System.out.println("__________________________________________________________________");
        System.out.println("a1:"+a1+" a2:"+a2);
        double f1 = ((double) a1.get("f1") + (double) a2.get("f1")) / 2.0;
        double precision = ((double) a1.get("p") + (double) a2.get("p")) / 2.0;
        double recall = ((double) a1.get("r") + (double) a2.get("r")) / 2.0;
        System.out.println("average f1:"+f1);
        System.out.println("average precision:"+precision);
        System.out.println("average recall:"+recall);
    }
    private static HashMap<String,Double> getAccuracy(List full, List shed) {
        List fulldata = full;
        //.subList(fullBeginLine,fullEndLine);
        List sheddata = shed;
        //.subList(shedBeginLine,shedEndLine);
        Iterator iteratorShed = sheddata.iterator();
        Iterator iteratorFull = fulldata.iterator();
        Map<Double,Integer> f1Result = new HashMap<>();
        Map<Double,Integer> precisionResult = new HashMap<>();
        Map<Double,Integer> recallResult = new HashMap<>();
        int miss = 0;   int failure = 0;
        int right;  String[] result;  double precision;
        double recall;  double f1;  int temp;
        int count = 0;
        HashMap<String,String[]> realmap = new HashMap<>();
        while (iteratorFull.hasNext()) {
            HashMap<String,String[]> fullmap = fix2((String) iteratorFull.next());
            for (String str : fullmap.keySet()) {
                if (!realmap.containsKey(str)) {
                    realmap.put(str,fullmap.get(str));
                }
            }
        }
        System.out.println("real map size:"+realmap.size());
        while (iteratorShed.hasNext()) {
            count++;
            HashMap<String,String[]> shedmap = fix2((String) iteratorShed.next());
            for (Map.Entry e : shedmap.entrySet()) {
                String shedkey = (String) e.getKey();
                String[] shedChars = (String[]) e.getValue();
                if (realmap.containsKey(shedkey)) {
                    String[] fullChars = realmap.get(shedkey);
                    if (fullChars.length == 1 && fullChars[0].isEmpty()) {
                        fullChars = new String[]{String.valueOf(sizeOfBitSet + 1)};
                    }
                    if (shedChars.length == 1 && shedChars[0].isEmpty()) {
                        shedChars = new String[]{String.valueOf(sizeOfBitSet + 1)};
                    }
                    BitSet sFull = new BitSet(sizeOfBitSet);
                    BitSet sShed = new BitSet(sizeOfBitSet);
                    Arrays.stream(Arrays.stream(fullChars).mapToInt(Integer::valueOf).toArray()).forEach(es -> sFull.set(es, true));
                    Arrays.stream(Arrays.stream(shedChars).mapToInt(Integer::valueOf).toArray()).forEach(es -> sShed.set(es, true));
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
                    //System.out.println(recall+"~"+precision+"~"+right+"~"+shedChars.length+"~"+fullChars.length);
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
                }
            }
        }
        System.out.println("count="+count+" failure="+failure+" miss="+miss);
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
        //finalResult /= total;
        finalResult /= realmap.size();
        System.out.println("f1 = "+finalResult);
        double pr = 0.0;
        for (Map.Entry entry : precisionResult.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            pr += ((Double) entry.getKey() * t);
        }
        //pr /= total;
        pr /= realmap.size();
        System.out.println("precision = "+pr);
        double r = 0.0;
        for (Map.Entry entry : recallResult.entrySet()) {
            double t = (1.0 * ((Integer) entry.getValue()));
            r += ((Double) entry.getKey() * t);
        }
        //r /= total;
        r /= realmap.size();
        System.out.println("recall = "+r);
        HashMap res = new HashMap();
        res.put("f1",finalResult);
        res.put("p",pr);
        res.put("r",r);
        return res;
    }

    @Test
    public void readLatency() {
        List fulldata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/20171023all/zuixin/juebugai/jcslatency", 100000);
        String[] res1;
        String[] res2;
        ArrayList<Double> ratios = new ArrayList<>();
        for (int i=0; i<fulldata.size()-1; i+=2) {
            res1 = ((String) fulldata.get(i)).split(":");
            res2 = ((String) fulldata.get(i+1)).split(":");
            double one = Double.valueOf(res1[1]);
            double two = Double.valueOf(res2[1]);
            double first = Double.valueOf(res1[0]);
            double second = Double.valueOf(res2[0]);
            double no1 = Double.valueOf(res1[2]);
            double no2 = Double.valueOf(res2[2]);
            double ans = ((first/(first+second))*one) + ((second/(first+second))*two);
            double ratio = ((no1/first)+(no2/second)) / 2.0;
            ratios.add(ratio);
            System.out.println(ans);
        }
        System.out.println("________________________");
        ratios.stream().forEach(e -> {
            System.out.println(e);
        });
    }

    @Test
    public void readLatency2() {
        List fulldata = TestWRInputFileForRedis
                .readFileByLine("/opt/oddata/20171023all/zuixin/zuixinlatency1", 100000);
        String[] res1;
        String[] res2;
        String[] res3;
        String[] res4;
        ArrayList<Double> ratios = new ArrayList<>();
        for (int i=0; i<fulldata.size()-3; i+=2) {
            res1 = ((String) fulldata.get(i)).split(":");
            res2 = ((String) fulldata.get(i+1)).split(":");
            res3 = ((String) fulldata.get(i+2)).split(":");
            res4 = ((String) fulldata.get(i+3)).split(":");
            double one = Double.valueOf(res1[1]);
            double two = Double.valueOf(res2[1]);
            double three = Double.valueOf(res3[1]);
            double four = Double.valueOf(res4[1]);
            double first = Double.valueOf(res1[0]);
            double second = Double.valueOf(res2[0]);
            double third = Double.valueOf(res3[0]);
            double fourth = Double.valueOf(res4[0]);
            double no1 = Double.valueOf(res1[2]);
            double no2 = Double.valueOf(res2[2]);
            double no3 = Double.valueOf(res3[2]);
            double no4 = Double.valueOf(res4[2]);
            double ans = ((first/(first+second+third+fourth))*one) + ((second/(first+second+third+fourth))*two)
                    + ((third/(first+second+third+fourth))*three) + ((fourth/(first+second+third+fourth))*four);
            double ratio = ((no1/first)+(no2/second)) / 2.0;
            ratios.add(ratio);
            System.out.println(ans);
        }
        System.out.println("________________________");
        ratios.stream().forEach(e -> {
            System.out.println(e);
        });
    }
}
