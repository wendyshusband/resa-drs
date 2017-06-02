package resa.optimize;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-5-6.
 */
public class BoltAggResult extends AggResult {

    private Map<String, CntMeanVar> tupleProcess = new HashMap<>();

    public Map<String, CntMeanVar> getTupleProcess() {
        return tupleProcess;
    }

    protected Map<String, Long> passiveSheddingCountMap = new HashMap<>();//tkl

    public CntMeanVar getCombinedProcessedResult() {
        CntMeanVar retVal = new CntMeanVar();
        tupleProcess.values().stream().forEach(retVal::addCMV);
        return retVal;
    }

    @Override
    public void add(AggResult r) {
        super.add(r);
        ((BoltAggResult) r).tupleProcess.forEach((s, cntMeanVar) ->
                this.tupleProcess.computeIfAbsent(s, (k) -> new CntMeanVar()).addCMV(cntMeanVar));
        ((BoltAggResult) r).getPassiveSheddingCountMap().forEach((stream,count)->{
            if(this.passiveSheddingCountMap.containsKey(stream)){
                passiveSheddingCountMap.put(stream,passiveSheddingCountMap.get(stream)+count);
            }else{
                passiveSheddingCountMap.put(stream,count);
            }
        });
        //System.out.println("heihei"+passiveSheddingCountMap);
    }

    public double getAvgServTimeHis(){
        return this.getCombinedProcessedResult().getAvg();
    }

    public double getScvServTimeHis(){
        return this.getCombinedProcessedResult().getScv();
    }

    public long getNumCompleteTuples(){
        return this.getCombinedProcessedResult().getCount();
    }

    public Map<String, Long> getPassiveSheddingCountMap() {
        return passiveSheddingCountMap;
    }


    public static void main(String[] args) {
//        List<String> list = new ArrayList<String>();
//        list.add( "0" );
//        list.add( "1" );
//        list.add( "2" );
//        list.add( "3" );
//        list.add( "4" );
//        list.add( "5" );
//        list.add( "6" );
//        list.add( "7" );
//        list.add( "8" );
//        list.add( "9" );
//        list.add( "10" );
//        list.add( "11" );
//        list.add( "12" );
//        list.add( "13" );
//        list.add( "14" );
//        list.add( "15" );
//        list.add( "16" );
//        list.add( "17" );
//        list.add( "18" );
//        list.add( "19" );
//        list.add( "20" );
//        System.out.println(list.size());
//        int i = 0;
//        Iterator<String> it = list.iterator();
//        while ( it.hasNext() ) {
//            String str = it.next();
//            if ( i % 2 == 0 ) {
//                //System.out.println( i + "===" + str );
//                it.remove();
//            }
//            i++;
//        }
//        System.out.println(list.size());
//        for ( String str : list ) {
//            // System.out.println( str );
//        }
//        HashMap<String,Integer> s = new HashMap<>();
//        s.put("1",100);s.put("2",1);s.put("3",1);s.put("4",1);s.put("5",1);s.put("6",1);s.put("7",1);s.put("8",1);
//        System.out.println(s.values().stream().mapToInt(Number::intValue).sum());
//        System.out.println(102<<1);
//        System.out.println(Math.log10(0));
//        String json = "{\"2\":[\"efg\"],\"1\":[\"abc\"],\"3\":[\"D-stream\",\"C-stream\"]}";
//        HashMap<String,List<String>> m = new HashMap<>();
//        JSONParser parser = new JSONParser();
//        try {
//            JSONObject sd = (JSONObject) parser.parse(json);
//            m = sd;
//            System.out.println(m.containsKey("2a"));
//            System.out.println(m.get("1").getClass());
//            for(Object array : m.values()){
//                JSONArray array1 = (JSONArray) array;
//                System.out.println(array1.get(0));
//            }
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        String s = "sort-as";
//        Pattern pattern1 = Pattern.compile(s+"=(\\d+)\\.(\\d+)");
//        Matcher matcher1 = pattern1.matcher("{sort-a=0.5,sort-BoltA=0.3}");
//        Pattern pattern2 = Pattern.compile("(\\d+)\\.(\\d+)");
//        matcher1.find();
//        Matcher matcher2 = pattern2.matcher(matcher1.group());
//        matcher2.find();
//        System.out.println(Double.valueOf(matcher2.group()));


        String sv = "3  .41";

        System.out.println(Integer.parseInt(sv));
    }
}
