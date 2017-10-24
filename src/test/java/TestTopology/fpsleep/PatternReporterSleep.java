package TestTopology.fpsleep;

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
import resa.shedding.tools.TestRedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-6-5.
 */
public class PatternReporterSleep extends TASleepBolt implements Constant {
    private static final Logger LOG = Logger.getLogger(PatternReporterSleep.class);

    private OutputCollector collector;
    private Map<Integer, String> invdict;

    public PatternReporterSleep(IntervalSupplier sleep) {
        super(sleep);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        int id = 0;
        invdict = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(
                //(String) stormConf.get(DICT_FILE_PROP) "/diu.yaml"
                new InputStreamReader(this.getClass().getResourceAsStream((String) stormConf.get(DICT_FILE_PROP))))) {
            String line;
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
        WordList wordList = (WordList) input.getValueByField(PATTERN_FIELD);
        List<String> words = IntStream.of(wordList.getWords()).mapToObj(invdict::get).collect(Collectors.toList());
        LOG.debug("In Reporter, " +System.currentTimeMillis()+ ":" + words + "," + input.getBooleanByField(IS_ADD_MFP));
        TestRedis.insertList("fpres", words + "," + input.getBooleanByField(IS_ADD_MFP));
        //TODO: use tuple tree to judge if the update belongs to the same tuple input events.
        collector.ack(input);
    }
//DateTime.now()
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PATTERN_FIELD, IS_ADD_MFP));
    }
}
