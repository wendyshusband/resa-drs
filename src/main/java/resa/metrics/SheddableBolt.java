package resa.metrics;

import org.apache.storm.topology.IRichBolt;
import resa.shedding.basicServices.IShedding;
import resa.shedding.tools.ISampler;
import resa.topology.DelegatedBolt;

/**
 * Created by 44931 on 2017/8/6.
 */
public class SheddableBolt extends DelegatedBolt {

    private IShedding _shedder;
    private ISampler _sampler;

    public SheddableBolt(IRichBolt bolt, IShedding shedder, ISampler sampler){
        super(bolt);
        this._shedder = shedder;
        this._sampler = sampler;
    }
}
