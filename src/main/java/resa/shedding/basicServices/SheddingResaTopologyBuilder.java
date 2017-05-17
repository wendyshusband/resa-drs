package resa.shedding.basicServices;

import org.apache.storm.topology.*;
import resa.metrics.MeasurableSpout;
import resa.shedding.DefaultSheddableBolt;

/**
 * Created by kailin on 28/3/17.
 */
public class SheddingResaTopologyBuilder extends TopologyBuilder {

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
        bolt = new DefaultSheddableBolt(bolt);
        return super.setBolt(id, bolt, parallelismHint);
    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
        spout = new MeasurableSpout(spout);
        return super.setSpout(id, spout, parallelismHint);
    }
}
