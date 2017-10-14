package TestTopology.noStaticMapFP;

import TestTopology.fp.Constant;
import TestTopology.fp.WordList;
import TestTopology.helper.IntervalSupplier;
import TestTopology.simulated.TASleepBolt;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.storm.serialization.SerializableSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by 44931 on 2017/9/19.
 */
public class FPtopDetector extends TASleepBolt implements Constant {

    private static final Logger LOG = LoggerFactory.getLogger(FPtopDetector.class);

    private PatterMap patterns;
    private int threshold;
    private OutputCollector collector;
  //  private List<Integer> targetTasks;

    public FPtopDetector(IntervalSupplier sleep) {
        super(sleep);
    }

    @DefaultSerializer(DefaultSerializers.KryoSerializableSerializer.class)
    public static class PatterMap extends ConcurrentHashMap<WordList, FPEntry> implements KryoSerializable {
        private long maxKeep;

        public PatterMap(long maxKeep) {
            super(65536, 0.75f);
            this.maxKeep = maxKeep;
        }

        public PatterMap() {
            this(Long.MAX_VALUE);
        }

        protected boolean removeEldestEntry(Entry eldest) {
            return System.currentTimeMillis() - ((FPEntry)eldest.getValue()).timestamp > maxKeep;
        }

        public void removeExpired(long now) {
            for (Iterator<Entry<WordList, FPEntry>> iter = entrySet().iterator(); iter.hasNext(); ) {
                Entry<WordList, FPEntry> e = iter.next();
                if (now - e.getValue().timestamp > maxKeep) {
                    iter.remove();
                } else {
                    return;
                }
            }
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeLong(maxKeep);
            output.writeInt(size());
            output.writeLong(System.currentTimeMillis());
            forEach((k, v) -> {
                kryo.writeClassAndObject(output, k);
                kryo.writeClassAndObject(output, v);
            });
            LOG.info("write out {} patterns", size());
        }

        @Override
        public void read(Kryo kryo, Input input) {
            maxKeep = Long.MAX_VALUE;
            long maxKeepTmp = input.readLong();
            int size = input.readInt();
            long last = input.readLong();
            // rest timestamp
            for (int i = 0; i < size; i++) {
                WordList p = (WordList) kryo.readClassAndObject(input);
                FPEntry entry = (FPEntry) kryo.readClassAndObject(input);
                put(p, entry);
            }
            long toAdd = System.currentTimeMillis() - last + 10000;
            forEach((k, v) -> v.setTimestamp(v.timestamp + toAdd));
            maxKeep = maxKeepTmp;
            LOG.info("read in {} patterns", size);
        }
    }

    @DefaultSerializer(SerializableSerializer.class)
    public static class FPEntry implements Serializable {
        int count = 0;
        boolean detectedBySelf;
        boolean flagMFPattern = false;
        long timestamp;

        public FPEntry(long timestamp) {
            this.timestamp = timestamp;
        }

        public long setTimestamp(long timestamp) {
            long tmp = this.timestamp;
            this.timestamp = timestamp;
            return tmp;
        }

        public void setDetectedBySelf(boolean detectedBySelf) {
            this.detectedBySelf = detectedBySelf;
        }

        public boolean isDetectedBySelf() {
            return detectedBySelf;
        }

        public void setMFPattern(boolean flag) {
            this.flagMFPattern = flag;
        }

        public boolean isMFPattern() {
            return this.flagMFPattern;
        }

        int getCount() {
            return count;
        }

        int incCountAndGet() {
            return ++count;
        }

        int decCountAndGet() {
            return --count;
        }

        boolean unused() {
            return count <= 0;
        }

        String reportCnt() {
            return String.format(" cnt: %d", this.count);
        }

        @Override
        public String toString() {
            return "{count: "+count+" timestamp: "+timestamp+" detectedBySelf: "+detectedBySelf+"}";
        }
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String patternData = "pattern";
        patterns = (PatterMap) context.getTaskData(patternData);
        if (patterns == null) {
            long maxKeepInterval = ConfigUtil.getInt(stormConf, MAX_KEEP_PROP, 60000);
            context.setTaskData(patternData, (patterns = new PatterMap(maxKeepInterval)));
        }
        this.collector = collector;
        this.threshold = ConfigUtil.getInt(stormConf, THRESHOLD_PROP, 20);
        LOG.info("In New Sleep Detector, threshold: " + threshold);
    }

    @Override
    public void execute(Tuple input) {
        super.execute(input);
        final long now = System.currentTimeMillis();
        ArrayList<WordList> wordListArrayList = (ArrayList<WordList>) input.getValueByField(PATTERN_FIELD);
        wordListArrayList.forEach((pattern) -> {
            long temp = now;
            FPEntry entry = patterns.computeIfAbsent(pattern, (k) -> new FPEntry(temp));
            if (input.getBooleanByField(IS_ADD_FIELD)) {
                entry.incCountAndGet();
                if (entry.getCount() >= threshold && !entry.isMFPattern()) {
                    entry.setMFPattern(true);
                    System.out.println("detectorsend+:"+Arrays.toString(pattern.getWords()));
                    collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, true));
                }
            } else {
                entry.decCountAndGet();
                if (entry.getCount() == threshold - 1) {
                    if (entry.isMFPattern()) {
                        entry.setMFPattern(false);
                        System.out.println("detectorsend-:"+Arrays.toString(pattern.getWords()));
                        collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, false));
                    }
                }
            }
            //getAndcheckSubPatternExcludeSelf(pattern.getWords(), collector, input, pattern);
            if (entry.unused()) {
                patterns.remove(pattern);
            } else {
                entry.setTimestamp(now);
            }
        });
        patterns.removeExpired(now);
        sleep(wordListArrayList.size());
        collector.ack(input);
    }

    private static void sleep(long t) {
        long t1 = System.currentTimeMillis();
        do {
            for (int i = 0; i < 10; i++) {
                Math.atan(Math.sqrt(Math.random() * Integer.MAX_VALUE));
            }
        } while (System.currentTimeMillis() - t1 < t);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(REPORT_STREAM, new Fields(PATTERN_FIELD, IS_ADD_MFP));
    }
}
