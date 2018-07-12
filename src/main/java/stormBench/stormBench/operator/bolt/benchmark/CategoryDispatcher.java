/**
 * 
 */
package stormBench.stormBench.operator.bolt.benchmark;

import java.util.Map;
import java.util.logging.Logger;

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
public class CategoryDispatcher implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7247785057317406219L;
	private OutputCollector collector;
	private static Logger logger = Logger.getLogger("CategoryDispatcher");
	

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		Integer age = input.getIntegerByField(FieldNames.AGE.toString());
		String city = input.getStringByField(FieldNames.CITY.toString());
		String opinion = input.getStringByField(FieldNames.OPINION.toString());
		this.collector.emit(FieldNames.AGE.toString(), input, new Values(age, opinion));
		this.collector.emit(FieldNames.CITY.toString(), input, new Values(city, opinion));
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		logger.fine("Cleaning up CategoryDispatcher " + serialVersionUID + "...");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(FieldNames.AGE.toString(), new Fields(FieldNames.AGE.toString(), FieldNames.OPINION.toString()));
		declarer.declareStream(FieldNames.CITY.toString(), new Fields(FieldNames.CITY.toString(), FieldNames.OPINION.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
