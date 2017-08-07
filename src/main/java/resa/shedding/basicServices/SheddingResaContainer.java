package resa.shedding.basicServices;

import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.FilteredMetricsCollector;
import resa.metrics.MeasuredData;
import resa.metrics.MetricNames;
import resa.shedding.tools.DRSzkHandler;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by kailin on 28/3/17.
 */
public class SheddingResaContainer extends FilteredMetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(SheddingResaContainer.class);
    public static final String METRIC_OUTPUT = "resa.container.metric.output";
    private SheddingResourceScheduler resourceScheduler = new SheddingResourceScheduler();
    private SheddingContainerContext ctx;
    private Nimbus.Client nimbus;
    private String topologyName;
    private String topologyId;
    private Map<String, Object> conf;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean outputMetrics;

    @Override
    public void prepare(Map conf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        super.prepare(conf, arg, context, errorReporter);
        this.conf = conf;
        this.topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        // connected to nimbus
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        topologyId = TopologyHelper.getTopologyId(nimbus, topologyName);
        // add approved metric name
        addApprovedMetirc("__sendqueue", MetricNames.SEND_QUEUE);
        addApprovedMetirc("__receive", MetricNames.RECV_QUEUE);
        addApprovedMetirc(MetricNames.COMPLETE_LATENCY);
        addApprovedMetirc(MetricNames.TASK_EXECUTE);
        addApprovedMetirc(MetricNames.EMIT_COUNT);
        addApprovedMetirc(MetricNames.DURATION);
        addApprovedMetirc(MetricNames.SHEDDING_RATE);
        addApprovedMetirc(MetricNames.FAILURE_COUNT);

        Map<String, Object> topo = buildTopologyInfo(context);
        ctx = new SheddingResaContainer.SheddingContainerContextImpl(context.getRawTopology(), conf, (Map<String, Object>) topo.get("targets"));
        // topology optimizer will start its own thread
        // if more services required to start, maybe we need to extract a new interface here
        resourceScheduler.init(ctx);
        resourceScheduler.start();

        outputMetrics = (Boolean) conf.getOrDefault(METRIC_OUTPUT, Boolean.FALSE);
        outputTopologyInfo(topo);
    }

    public Map<String, Object> buildTopologyInfo(TopologyContext context){
        Map<String, Object> topo = new LinkedHashMap<>();
        topo.put("id", context.getStormId());
        topo.put("spouts", new ArrayList<>(context.getRawTopology().get_spouts().keySet()));
        topo.put("bolts", new ArrayList<>(context.getRawTopology().get_bolts().keySet()));
        topo.put("targets", new HashMap<>());
        for (String comp : context.getComponentIds()) {
            Map<String, List<String>> targets = new HashMap<>();
            context.getTargets(comp).forEach((stream, g) -> {
                targets.put(stream, new ArrayList<>(g.keySet()));
            });
            ((Map<String, Object>) topo.get("targets")).put(comp, targets);
        }
        return topo;
    }
    private void outputTopologyInfo(Map<String, Object> topo) {
        ctx.emitMetric("topology.info", topo);
    }

    private String object2Json(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class SheddingContainerContextImpl extends SheddingContainerContext {

        protected SheddingContainerContextImpl(StormTopology stormTopology, Map<String, Object> conf,Map<String, Object> targets) {
            super(stormTopology, conf, targets);
        }


        @Override
        public void emitMetric(String name, Object data) {
            if (outputMetrics) {
                String val = name + "-->" + object2Json(data);
                LOG.info(val);
            }
        }

        @Override
        public synchronized Map<String, List<ExecutorDetails>> runningExecutors() {
            return TopologyHelper.getTopologyExecutors(nimbus, topologyId).entrySet().stream()
                    .filter(e -> !Utils.isSystemId(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public boolean requestRebalance(Map<String, Integer> allocation, int numWorkers) {
            RebalanceOptions options = new RebalanceOptions();
            //set rebalance options
            options.set_num_workers(numWorkers);
            options.set_num_executors(allocation);
            int waitingSecs = ConfigUtil.getInt(conf, ResaConfig.REBALANCE_WAITING_SECS, -1);
            if (waitingSecs >= 0) {
                options.set_wait_secs(waitingSecs);
            }
            try {
                nimbus.rebalance(topologyName, options);
                LOG.info("Do rebalance successfully for topology " + topologyName);
                return true;
            } catch (Exception e) {
                LOG.warn("Do rebalance failed for topology " + topologyName, e);
            }
            return false;
        }
    }

    @Override
    protected void handleSelectedDataPoints(IMetricsConsumer.TaskInfo taskInfo,
                                            Collection<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Object> ret = dataPoints.stream().collect(Collectors.toMap(p -> p.name, p -> p.value));
        MeasuredData measuredData = new MeasuredData(taskInfo.srcComponentId, taskInfo.srcTaskId,
                taskInfo.timestamp, ret);
        //LOG.info(measuredData.data.toString()+"chongge"+measuredData.task+"t"+
        //  measuredData.component+"i"+measuredData.timestamp);//tkl
        ctx.getListeners().forEach(l -> l.measuredDataReceived(measuredData));
    }
    @Override
    public void cleanup() {
        super.cleanup();
        resourceScheduler.stop();
        try {
            DRSzkHandler.close("/drs");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
