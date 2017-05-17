package resa.shedding.basicServices;

import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.MeasurableSpout;
import resa.shedding.DefaultSheddableBolt;

import java.util.Map;

/**
 * Created by kailin on 28/3/17.
 */
public class SheddingResaTopologyValidator implements ITopologyValidator {

    private static final Logger LOG = LoggerFactory.getLogger(SheddingResaTopologyValidator.class);

    @Override
    public void prepare(Map StormConf) {
        LOG.info("Preparing SheddingResaTopologyValidator");
    }

    @Override
    public void validate(String topologyName, Map topologyConf, StormTopology topology) throws InvalidTopologyException {

        topology.get_spouts().forEach((k, v) -> {
            MeasurableSpout s = new MeasurableSpout();
            s.setSerializedSpout(v.get_spout_object().get_serialized_java());
            v.set_spout_object(ComponentObject.serialized_java(Utils.javaSerialize(s)));
        });

        topology.get_bolts().forEach((k, v) -> {
            DefaultSheddableBolt b = new DefaultSheddableBolt();
            b.setSerializedBolt(v.get_bolt_object().get_serialized_java());
            v.set_bolt_object(ComponentObject.serialized_java(Utils.javaSerialize(b)));
        });
    }
}