package resa.shedding.basicServices;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.ExecutorDetails;
import resa.metrics.MeasuredData;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by kailin on 29/3/17.
 */
public abstract class SheddingContainerContext {
    public static interface Listener {
        void measuredDataReceived(MeasuredData measuredData);
    }

    private StormTopology topology;
    private Map<String, Object> conf;
    private Map<String, Object> targets;

    private final Set<SheddingContainerContext.Listener> listeners = new CopyOnWriteArraySet<>();

    protected SheddingContainerContext(StormTopology topology, Map<String, Object> conf,Map<String, Object> targets) {
        this.topology = topology;
        this.conf = conf;
        this.targets = targets;
    }

    public abstract void emitMetric(String name, Object data);

    public void addListener(SheddingContainerContext.Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(SheddingContainerContext.Listener listener) {
        listeners.remove(listener);
    }

    protected Set<SheddingContainerContext.Listener> getListeners() {
        return listeners;
    }

    public Map<String, Object> getConfig() {
        return conf;
    }

    public Map<String, Object> getTargets() {
        return targets;
    }

    public StormTopology getTopology() {
        return topology;
    }

    public abstract Map<String, List<ExecutorDetails>> runningExecutors();

    public abstract boolean requestRebalance(Map<String, Integer> allocation, int numWorkers);


}
