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
public class RegistrationProcessor implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2401068308616929944L;
	
	private OutputCollector collector;
	private HashMap<Integer, String> drivers;

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.drivers = new HashMap<>();
		this.drivers.put(0, "Oscar");
		this.drivers.put(1, "Alice");
		this.drivers.put(2, "Bob");
		this.drivers.put(3, "Charles");
		this.drivers.put(4, "Daniel");
		this.drivers.put(5, "Erin");
		this.drivers.put(6, "Francine");
		this.drivers.put(7, "Gregory");
		this.drivers.put(8, "Hubert");
		this.drivers.put(9, "Idriss");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String registration = input.getStringByField(FieldNames.REGISTR.toString());
		Integer driverId = Integer.parseInt(registration.substring(0, 1));
		String driver = this.drivers.get(driverId);
		if(driver != null){
			this.collector.emit(new Values(driver));
			try {
				Thread.sleep(60);
			} catch (InterruptedException e) {
				e.printStackTrace();
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
		declarer.declare(new Fields(FieldNames.DRIVER.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
