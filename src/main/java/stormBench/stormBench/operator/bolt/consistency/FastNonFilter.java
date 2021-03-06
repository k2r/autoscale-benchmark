/**
 * 
 */
package stormBench.stormBench.operator.bolt.consistency;

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
public class FastNonFilter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 137487965305521668L;
	OutputCollector collector;
	
	private static Logger logger = Logger.getLogger("FastNonFilter");

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		this.collector.emit(input, new Values(1));
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			logger.severe("FastNonFilter was interrupted because of " + e);
		}
		this.collector.ack(input);
	}

	@Override
	public void cleanup() {
		logger.info("Cleaning FastNonFilter bolt...");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.ID.toString()));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}