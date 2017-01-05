/**
 * 
 */
package stormBench.stormBench.operator.bolt.radar;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import stormBench.stormBench.utils.FieldNames;

/**
 * @author Roland
 *
 */
public class DriverProcessor implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7749654307345810531L;

	private OutputCollector collector;
	private HashMap<String, Integer> violationCounts;
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.violationCounts = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String driver = input.getStringByField(FieldNames.DRIVER.toString());
		if(!this.violationCounts.containsKey(driver)){
			this.violationCounts.put(driver, 0);
		}
		Integer vcount = this.violationCounts.get(driver);
		vcount++;
		this.violationCounts.put(driver, vcount);
		this.collector.ack(input);
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
