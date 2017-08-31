package resa.shedding.basicServices.api;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import resa.optimize.AggResult;
import resa.shedding.tools.DRSzkHandler;

import java.util.List;
import java.util.Map;

/**
 * Created by 44931 on 2017/7/29.
 */
public abstract class ActiveSheddingRateTrimer {

    protected StormTopology rawTopology;
    protected Map<String, Object> conf;
    protected transient CuratorFramework client;
    protected String topologyName;

    public void init(Map<String, Object> conf, StormTopology topology) {
        this.conf = conf;
        this.rawTopology = topology;
        List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        int port = Math.toIntExact((long) conf.get(Config.STORM_ZOOKEEPER_PORT));
        client = DRSzkHandler.newClient(zkServer.get(0).toString(), port, 6000, 6000, 1000, 3);
        topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
    }

    public abstract void trim(Map<String, AggResult[]> comp2ExecutorResults);


}
