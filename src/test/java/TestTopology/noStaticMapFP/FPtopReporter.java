package TestTopology.noStaticMapFP;

import TestTopology.fp.Constant;
import TestTopology.fp.WordList;
import TestTopology.helper.IntervalSupplier;
import TestTopology.simulated.TASleepBolt;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import resa.shedding.tools.TestRedis;
import resa.util.ConfigUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-6-5.
 */
public class FPtopReporter extends TASleepBolt implements Constant {
    private static final Logger LOG = Logger.getLogger(FPtopReporter.class);
    private int threshold;
    private OutputCollector collector;
    private Map<Integer, String> invdict;
    //private static FPtopDetector.PatterMap patterns;
    private HashMap<WordList, Boolean> map;

    public FPtopReporter(IntervalSupplier sleep) {
        super(sleep);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//        String patternData = "pattern";
//        patterns = (FPtopDetector.PatterMap) context.getTaskData(patternData);
//        if (patterns == null) {
//            long maxKeepInterval = ConfigUtil.getInt(stormConf, MAX_KEEP_PROP, 60000);
//            context.setTaskData(patternData, (patterns = new FPtopDetector.PatterMap(maxKeepInterval)));
//        }
        this.threshold = ConfigUtil.getInt(stormConf, THRESHOLD_PROP, 20);
        this.collector = collector;
        map = new HashMap<>();
        int id = 0;
        invdict = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream((String) stormConf.get(DICT_FILE_PROP))))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                invdict.put(id++, line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        super.execute(input);
        //System.out.println(input.toString()+"ganniniang!!!"+input.getBooleanByField(IS_ADD_MFP));
        WordList wordList = (WordList) input.getValueByField(PATTERN_FIELD);
        if (input.getBooleanByField(IS_ADD_MFP)) {
            System.out.println("+ganniniang~~"+input.getBooleanByField(IS_ADD_MFP)+"super:"+haveSuperPatternFP(wordList)+"contains:"+map.containsKey(wordList)+" is:"+map.get(wordList));
            if (!haveSuperPatternFP(wordList) && (!map.containsKey(wordList) || !map.get(wordList))) {
                map.put(wordList, true);
                List<String> words = IntStream.of(wordList.getWords()).mapToObj(invdict::get).collect(Collectors.toList());
                System.out.println("+ganniniang"+words.get(0));
                TestRedis.insertList("fpres", words + "," + input.getBooleanByField(IS_ADD_MFP));
                subSetTrueToFalse(wordList);
            } else {
                map.put(wordList, false);
            }
        } else {
            System.out.println("-ganniniang~~"+input.getBooleanByField(IS_ADD_MFP)+"super:"+haveSuperPatternFP(wordList)+"contains:"+map.containsKey(wordList)+" is:"+map.get(wordList));
            if (map.containsKey(wordList)) {
                map.remove(wordList);
                List<String> words = IntStream.of(wordList.getWords()).mapToObj(invdict::get).collect(Collectors.toList());
                System.out.println("-ganniniang"+words.get(0));
                TestRedis.insertList("fpres", words + "," + input.getBooleanByField(IS_ADD_MFP));
                subSetFalseToTrue(wordList);
            }
        }

        //TODO: use tuple tree to judge if the update belongs to the same tuple input events.
        collector.ack(input);
        collector.emit(new Values(wordList));
    }

    private void subSetTrueToFalse(WordList wordList) {
        int[] wordIds = wordList.getWords();
        int n = wordIds.length;
        int[] buffer = new int[n];
        for (int i = 1; i < (1 << n) - 1; i++) {
            int k = 0;
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    buffer[k++] = wordIds[j];
                }
            }
            WordList subPattern = new WordList(Arrays.copyOf(buffer, k));
            System.out.println("subSetTrueToFalse super:"+haveSuperPatternFP(subPattern)+"contains:"+map.containsKey(subPattern)+" is："+map.get(subPattern));
            if (map.containsKey(subPattern) && map.get(subPattern)) {
                map.put(subPattern,false);
                List<String> words = IntStream.of(wordList.getWords()).mapToObj(invdict::get).collect(Collectors.toList());
                TestRedis.insertList("fpres", words + "," + false);
            }
        }
    }

    private void subSetFalseToTrue(WordList wordList) {
        int[] wordIds = wordList.getWords();
        int n = wordIds.length;
        int[] buffer = new int[n];
        for (int i = 1; i < (1 << n) - 1; i++) {
            int k = 0;
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    buffer[k++] = wordIds[j];
                }
            }
            WordList subPattern = new WordList(Arrays.copyOf(buffer, k));
            System.out.println("subSetFalseToTrue super:"+haveSuperPatternFP(subPattern)+"contains:"+map.containsKey(subPattern)+" is："+map.get(subPattern));
            if (map.containsKey(subPattern) && !map.get(subPattern) && !haveSuperPatternFP(subPattern)) {
                map.put(subPattern,true);
                List<String> words = IntStream.of(wordList.getWords()).mapToObj(invdict::get).collect(Collectors.toList());
                TestRedis.insertList("fpres", words + "," + true);
            }
        }
    }

    private boolean haveSuperPatternFP(WordList pattern) {
        for (Iterator<Map.Entry<WordList, Boolean>> iter = map.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<WordList, Boolean> e = iter.next();
            if (e.getKey().compare(pattern) == 1 && e.getValue()) {// current pattern have super set.
                return true;
            }
        }
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PATTERN_FIELD, IS_ADD_MFP));
    }
}
