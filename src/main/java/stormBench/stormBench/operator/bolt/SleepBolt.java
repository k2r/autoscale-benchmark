/**
 * 
 */
package stormBench.stormBench.operator.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * @author Roland
 *
 */
public class SleepBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2109073700105983429L;
	private int sleepTime;
	private OutputCollector collector;
	
	public SleepBolt(int duration) {
		this.sleepTime = duration;
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		try {
			Thread.sleep(this.sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
