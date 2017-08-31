package resa.metrics;

import org.apache.storm.topology.IRichBolt;
import resa.shedding.basicServices.api.IShedding;
import resa.shedding.tools.AbstractSampler;
import resa.topology.DelegatedBolt;

/**
 * Created by 44931 on 2017/8/6.
 */
public class SheddableBolt extends DelegatedBolt {

    private IShedding _shedder;
    private AbstractSampler _sampler;

    public SheddableBolt(IRichBolt bolt, IShedding shedder, AbstractSampler sampler){
        super(bolt);
        this._shedder = shedder;
        this._sampler = sampler;
    }
}
