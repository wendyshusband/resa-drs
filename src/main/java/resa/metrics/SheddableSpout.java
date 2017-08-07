package resa.metrics;

import org.apache.storm.topology.IRichSpout;
import resa.shedding.basicServices.IShedding;
import resa.shedding.tools.ISampler;
import resa.topology.DelegatedSpout;

/**
 * Created by 44931 on 2017/8/6.
 */
public class SheddableSpout extends DelegatedSpout {

    private IShedding _shedder;
    private ISampler _sampler;

    public SheddableSpout(IRichSpout spout, IShedding shedder, ISampler sampler){
        super(spout);
        this._shedder = shedder;
        this._sampler = sampler;
    }
}
