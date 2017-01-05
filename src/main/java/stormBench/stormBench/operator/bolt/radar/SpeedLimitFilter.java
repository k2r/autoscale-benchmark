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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import stormBench.stormBench.utils.FieldNames;

/**
 * @author Roland
 *
 */
public class SpeedLimitFilter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5165262448662846097L;
	
	private OutputCollector collector;
	private HashMap<String, Integer> limits;

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.limits = new HashMap<>();
		this.limits.put("NORTH", 70);
		this.limits.put("EAST", 30);
		this.limits.put("WEST", 50);
		this.limits.put("SOUTH", 90);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		Integer speed = input.getIntegerByField(FieldNames.SPEED.toString());
		String location = input.getStringByField(FieldNames.LOC.toString());
		Integer limitByLocation = this.limits.get(location);
		if(limitByLocation != null){
			if(speed > limitByLocation){
				String registration = input.getStringByField(FieldNames.REGISTR.toString());
				this.collector.emit(new Values(registration, location));
			}
		}
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
		declarer.declare(new Fields(FieldNames.REGISTR.toString(), FieldNames.LOC.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
