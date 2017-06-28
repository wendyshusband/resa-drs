package resa.util;

import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import resa.shedding.basicServices.SheddingResaContainer;

import java.util.Map;

/**
 * Created by ding on 14-4-26.
 */
public class ResaConfig extends Config {

    /* start obsolete part */
    //The following two are not necessary in the new queue-related metric implementation.
    public static final String TRACE_COMP_QUEUE = "topology.queue.trace";
    public static final String COMP_QUEUE_SAMPLE_RATE = "resa.comp.queue.sample.rate";
    /* end obsolete part */

    public static final String MAX_EXECUTORS_PER_WORKER = "resa.topology.max.executor.per.worker";
    public static final String REBALANCE_WAITING_SECS = "resa.topology.rebalance.waiting.secs";
    public static final String ALLOWED_EXECUTOR_NUM = "resa.topology.allowed.executor.num";

    public static final String COMP_SAMPLE_RATE = "resa.comp.sample.rate";

    public static final String ALLOC_CALC_CLASS = "resa.optimize.alloc.class";
    public static final String SERVICE_MODEL_CLASS = "resa.optimize.service.model.class";
    public static final String OPTIMIZE_INTERVAL = "resa.optimize.interval.secs";
    public static final String OPTIMIZE_MIN_EXPECTED_REBALANCE_INTERVAL = "resa.opt.adjust.min.sec";
    public static final String OPTIMIZE_REBALANCE_TYPE = "resa.opt.adjust.type";
    public static final String OPTIMIZE_WIN_HISTORY_SIZE = "resa.opt.win.history.size";
    public static final String OPTIMIZE_WIN_HISTORY_SIZE_IGNORE = "resa.opt.win.history.size.ignore";
    public static final String OPTIMIZE_SMD_QOS_MS = "resa.opt.smd.qos.ms";
    public static final String OPTIMIZE_SMD_QOS_UPPER_MS = "resa.opt.smd.qos.upper.ms";
    public static final String OPTIMIZE_SMD_QOS_LOWER_MS = "resa.opt.smd.qos.lower.ms";
    public static final String OPTIMIZE_SMD_SEND_QUEUE_THRESH = "resa.opt.smd.sq.thresh";
    public static final String OPTIMIZE_SMD_RECV_QUEUE_THRESH_RATIO = "resa.opt.smd.rq.thresh.ratio";
    public static final String OPTIMIZE_SMD_RESOURCE_UNIT = "resa.opt.smd.resource.unit";

    public static final String ZK_ROOT_PATH = "resa.scheduler.zk.root";
    public static final String DECISION_MAKER_CLASS = "resa.scheduler.decision.class";

    //load shedding
    public static final String TUPLE_QUEUE_CAPACITY = "resa.shedding.tuple.queue.capacity";
    public static final String LAMBDA_FOR_SELECTIVITY_HISTORY_SIZE = "resa.shedding.lambda.selectivity.history.size";
    public static final String SELECTIVITY_FUNCTION_ORDER = "resa.shedding.selectivity.function.order";
    public static final String SELECTIVITY_CALC_CLASS = "resa.shedding.selectivity.calc.class";
    public static final String SHEDDING_THRESHOLD = "resa.shedding.thresdhold";
    public static final String ACTIVE_SHEDDING_MAP = "resa.shedding.active.stream.map";
    public static final String SHEDDING_ALLOC_CALC_CLASS = "resa.shedding.alloc.class";
    public static final String SHEDDING_DECISION_MAKER_CLASS = "resa.shedding.decision.class";
    public static final String SHEDDING_SERVICE_MODEL_CLASS = "resa.shedding.service.model.class";
    public static final String SPOUT_MAX_PENDING = "resa.spout.max.pending";
    public static final String SPOUT_PENDING_THRESHOLD = "resa.spout.pending.threshold";
    //RED
    public static final String HIGH_SHEDDING_THRESHOLD = "resa.shedding.high.thresdhold";
    public static final String LOW_SHEDDING_THRESHOLD = "resa.shedding.low.thresdhold";
    public static final String MAX_SHED_RATE = "resa.max.shed.rate";
    private ResaConfig(boolean loadDefault) {
        if (loadDefault) {
            //read default.yaml & storm.yaml
            try {
                putAll(Utils.readStormConfig());
            } catch (Throwable e) {
            }
        }
        Map<String, Object> conf = Utils.findAndReadConfigFile("resa.yaml", false);
        if (conf != null) {
            putAll(conf);
        }
    }

    /**
     * Create a new Conf, then load default.yaml and storm.yaml
     *
     * @return
     */
    public static ResaConfig create() {
        return create(false);
    }

    /**
     * Create a new resa Conf
     *
     * @param loadDefault
     * @return
     */
    public static ResaConfig create(boolean loadDefault) {
        return new ResaConfig(loadDefault);
    }

    public void addDrsSupport() {
        //registerMetricsConsumer(ResaContainer.class, 1);
        registerMetricsConsumer(SheddingResaContainer.class, 1);//tkl
    }
}
