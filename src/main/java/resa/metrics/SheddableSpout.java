package resa.metrics;

import org.apache.storm.topology.IRichSpout;
import resa.shedding.basicServices.api.IShedding;
import resa.shedding.tools.AbstractSampler;
import resa.topology.DelegatedSpout;

/**
 * Created by 44931 on 2017/8/6.
 */
public class SheddableSpout extends DelegatedSpout {

    private IShedding _shedder;
    private AbstractSampler _sampler;

    public SheddableSpout(IRichSpout spout, IShedding shedder, AbstractSampler sampler){
        super(spout);
        this._shedder = shedder;
        this._sampler = sampler;
    }
}
