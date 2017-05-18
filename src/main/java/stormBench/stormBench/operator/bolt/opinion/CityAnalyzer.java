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
public class CityAnalyzer implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1703486836254010868L;
	private OutputCollector collector;
	private static Logger logger = Logger.getLogger("CityAnalyzer");
	
	private HashMap<String, HashMap<String, Integer>> counts;//city -> opinion -> count
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
		String city = input.getStringByField(FieldNames.NORMCITY.toString());
		String opinion = input.getStringByField(FieldNames.OPINION.toString());
		Integer count = 0;
		if(counts.containsKey(city)){
			HashMap<String, Integer> cityMap = counts.get(city);
			if(cityMap.containsKey(opinion)){
				count = cityMap.get(opinion);
			}
		}
		count++;
		this.total++;
		HashMap<String, Integer> newCityMap = new HashMap<>();
		newCityMap.put(opinion, count);
		counts.put(city, newCityMap);
		Double percentage = count / this.total;
		this.collector.emit(input, new Values(city, opinion, percentage));
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		logger.fine("Cleaning up CityAnalyzer " + serialVersionUID + "...");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(FieldNames.NORMCITY.toString(), new Fields(FieldNames.NORMCITY.toString(), FieldNames.OPINION.toString(), FieldNames.PERCENT.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
