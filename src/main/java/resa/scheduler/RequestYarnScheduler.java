package resa.scheduler;

import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by kailin on 12/5/17.
 */
public class RequestYarnScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(RequestYarnScheduler.class);
    private Nimbus.Iface nimbus;
    private String zkServer;
    @Override
    public void prepare(Map conf) {
        List zkServer = (List) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        this.zkServer = (String) zkServer.get(0);
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("RequestYarnScheduler request yarn for new physical resource!");
        int topologiesTotalUsedSlots2 = 0;
        for(TopologyDetails topology : topologies.getTopologies()){
            System.out.println("worker: "+topology.getNumWorkers());
            System.out.println("executor: "+topology.getExecutors());
            topologiesTotalUsedSlots2+= topology.getNumWorkers();
        }
        int totalSlots = cluster.getAvailableSlots().size()+cluster.getUsedSlots().size();
        int topologiesTotalUsedSlots = topologies.getTopologies().stream().mapToInt(TopologyDetails::getNumWorkers).sum();
        System.out.println("topologiesTotalUsedSlots: "+topologiesTotalUsedSlots);
        System.out.println("topologiesTotalUsedSlots2: "+topologiesTotalUsedSlots2);
        System.out.println("all slots: "+totalSlots);
        if (totalSlots < topologiesTotalUsedSlots2) {

        }
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        new EvenScheduler().schedule(topologies, cluster);
    }
}
