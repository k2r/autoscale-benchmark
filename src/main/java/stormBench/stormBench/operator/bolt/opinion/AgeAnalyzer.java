/**
 * 
 */
package stormBench.stormBench.operator.bolt.opinion;

import java.util.HashMap;
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
public class AgeAnalyzer implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4375654363265106627L;
	private OutputCollector collector;
	private static Logger logger = Logger.getLogger("AgeAnalyzer");
	
	private HashMap<String, HashMap<String, Integer>> counts;//age category -> opinion -> count
	private Double total;
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		counts = new HashMap<>();
		total = 0.0;
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String category = input.getStringByField(FieldNames.CATAGE.toString());
		String opinion = input.getStringByField(FieldNames.OPINION.toString());
		Integer count = 0;
		if(counts.containsKey(category)){
			HashMap<String, Integer> catMap = counts.get(category);
			if(catMap.containsKey(opinion)){
				count = catMap.get(opinion);
			}
		}
		count++;
		this.total++;
		HashMap<String, Integer> newCatMap = new HashMap<>();
		newCatMap.put(opinion, count);
		counts.put(category, newCatMap);
		Double percentage = count / this.total;
		this.collector.emit(input, new Values(category, opinion, percentage));
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		logger.fine("Cleaning up AgeAnalyzer " + serialVersionUID + "...");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(FieldNames.CATAGE.toString(), new Fields(FieldNames.CATAGE.toString(), FieldNames.OPINION.toString(), FieldNames.PERCENT.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
