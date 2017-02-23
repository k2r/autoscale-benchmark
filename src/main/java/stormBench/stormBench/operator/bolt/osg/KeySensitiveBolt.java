/**
 * 
 */
package stormBench.stormBench.operator.bolt.osg;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * @author Roland
 *
 */
public class KeySensitiveBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1652575920744820151L;
	private ZipfDistribution zipf;
	private OutputCollector collector;
	HashMap<Integer, Integer> processingTimes;

	public KeySensitiveBolt(Integer nbProcTimes, Double skew) {
		this.zipf = new ZipfDistribution(nbProcTimes, skew);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.processingTimes = new HashMap<>();
		this.collector = collector;
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		Integer value = input.getIntegerByField("value");
		if(!this.processingTimes.containsKey(value)){
			this.processingTimes.put(value, this.zipf.sample());
		}
		Integer processingTime = this.processingTimes.get(value);
		try {
			Thread.sleep(processingTime);
			this.collector.ack(input);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
