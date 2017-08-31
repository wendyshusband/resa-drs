package resa.shedding.basicServices;

import org.apache.storm.topology.*;
import resa.metrics.SheddableBolt;
import resa.metrics.SheddableSpout;
import resa.shedding.basicServices.api.IShedding;
import resa.shedding.tools.AbstractSampler;

/**
 * Created by kailin on 28/3/17.
 */
public class SheddingResaTopologyBuilder extends TopologyBuilder {

    IShedding _shedder;
    AbstractSampler _sampler;

    public SheddingResaTopologyBuilder (IShedding shedder, AbstractSampler sampler) {
        _sampler = sampler;
        _shedder = shedder;
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
        bolt = new SheddableBolt(bolt, _shedder, _sampler);
        return super.setBolt(id, bolt, parallelismHint);
    }

//    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
//        bolt = new DefaultSheddableBolt(bolt);
//        return super.setBolt(id, bolt, parallelismHint);
//    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
        spout = new SheddableSpout(spout, _shedder, _sampler);
        return super.setSpout(id, spout, parallelismHint);
    }
}
