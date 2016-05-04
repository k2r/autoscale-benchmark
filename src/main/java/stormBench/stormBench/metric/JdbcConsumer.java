/**
 * 
 */
package stormBench.stormBench.metric;

import java.util.Collection;
import java.util.Map;

import backtype.storm.generated.Grouping;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import stormBench.stormBench.utils.MetricNames;

/**
 * @author Roland
 *
 */
public class JdbcConsumer implements IMetricsConsumer {
	
	/**
	 * 
	 */
	public JdbcConsumer() {
	}

	/* (non-Javadoc)
	 * @see backtype.storm.metric.api.IMetricsConsumer#prepare(java.util.Map, java.lang.Object, backtype.storm.task.TopologyContext, backtype.storm.task.IErrorReporter)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
			IErrorReporter errorReporter) {
	}

	/* (non-Javadoc)
	 * @see backtype.storm.metric.api.IMetricsConsumer#handleDataPoints(backtype.storm.metric.api.IMetricsConsumer.TaskInfo, java.util.Collection)
	 */
	@Override
	public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		for(DataPoint data : dataPoints){
			String name = data.name;
			if(name.equalsIgnoreCase(MetricNames.CPU.toString())){
				long timestamp = taskInfo.timestamp;
				String host = taskInfo.srcWorkerHost;
				int port = taskInfo.srcWorkerPort;
				String component = taskInfo.srcComponentId;
				int task = taskInfo.srcTaskId;
				//TODO make the storage of the scheduling and the cpu load with a jdbc connector
			}
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.metric.api.IMetricsConsumer#cleanup()
	 */
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
