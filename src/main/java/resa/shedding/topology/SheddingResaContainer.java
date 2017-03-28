package resa.shedding.topology;

import org.apache.storm.metric.api.IMetricsConsumer;
import resa.metrics.FilteredMetricsCollector;

import java.util.Collection;

/**
 * Created by kailin on 28/3/17.
 */
public class SheddingResaContainer extends FilteredMetricsCollector {
    @Override
    protected void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

    }
}
