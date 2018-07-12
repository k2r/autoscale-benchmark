/**
 * 
 */
package stormBench.stormBench.operator.bolt.consistency;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.random.JDKRandomGenerator;
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
public class FastFilter implements IRichBolt {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4163204192485205823L;
	OutputCollector collector;
	JDKRandomGenerator generator;
	private static Logger logger = Logger.getLogger("FastFilter");
	
	public FastFilter() {
		this.generator = new JDKRandomGenerator(49991);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Integer filter = this.generator.nextInt(10);
		if(filter == 0){
			this.collector.emit(input, new Values(1));
		}
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			logger.severe("FastNonFilter was interrupted because of " + e);
		}
		this.collector.ack(input);	
	}

	@Override
	public void cleanup() {
		logger.info("Cleaning FastFilter bolt...");
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
