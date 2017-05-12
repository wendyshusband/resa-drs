package resa.scheduler;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;

/**
 * Created by kailin on 12/5/17.
 */
public class RequestYarnScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(RequestYarnScheduler.class);

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("");
    }
}
