/**
 * 
 */
package stormBench.stormBench.operator.bolt.opinion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import stormBench.stormBench.socket.SocketSource;
import stormBench.stormBench.utils.FieldNames;

/**
 * @author Roland
 *
 */
public class OpinionAnalyzer implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3146352036240971007L;
	private static Logger logger = Logger.getLogger("OpinionAnalyzer");
	private OutputCollector collector;
	
	private Integer port;
	private SocketSource source;
	private HashMap<String, HashMap<String, Double>> dominantCategories;//opinion -> age category -> percentage
	private HashMap<String, HashMap<String, Double>> dominantCities;//opinion -> city -> percentage
	

	public OpinionAnalyzer(Integer port) {
		this.port = port;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.source = new SocketSource(this.port);
		this.dominantCategories = new HashMap<>();
		this.dominantCities = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String stream = input.getSourceStreamId();
		if(stream.equalsIgnoreCase(FieldNames.CATAGE.toString())){
			String category = input.getStringByField(FieldNames.CATAGE.toString());
			String opinion = input.getStringByField(FieldNames.OPINION.toString());
			Double percent = input.getDoubleByField(FieldNames.PERCENT.toString());
			boolean isMax = true;
			if(dominantCategories.containsKey(opinion)){
				HashMap<String, Double> categories = dominantCategories.get(opinion);
				Set<String> keySet = categories.keySet();
				for(String key : keySet){
					Double currentMax = categories.get(key);
					if(currentMax >= percent){
						isMax = false;
						break;
					}
				}
				if(isMax){
					HashMap<String, Double> newMaxCategory = new HashMap<>();
					newMaxCategory.put(category, percent);
					dominantCategories.put(opinion, newMaxCategory);
					ArrayList<String> batch = new ArrayList<>();
					String info = "Opinion " + opinion + " is correlated to age in " + category + " (" + percent + "%)"; 
					batch.add(info);
					this.source.sendBatch(batch);
				}
			}
		}
		if(stream.equalsIgnoreCase(FieldNames.NORMCITY.toString())){
			String city = input.getStringByField(FieldNames.NORMCITY.toString());
			String opinion = input.getStringByField(FieldNames.OPINION.toString());
			Double percent = input.getDoubleByField(FieldNames.PERCENT.toString());
			boolean isMax = true;
			if(dominantCities.containsKey(opinion)){
				HashMap<String, Double> cities = dominantCities.get(opinion);
				Set<String> keySet = cities.keySet();
				for(String key : keySet){
					Double currentMax = cities.get(key);
					if(currentMax >= percent){
						isMax = false;
						break;
					}
				}
				if(isMax){
					HashMap<String, Double> newMaxCity = new HashMap<>();
					newMaxCity.put(city, percent);
					dominantCities.put(opinion, newMaxCity);
					ArrayList<String> batch = new ArrayList<>();
					String info = "Opinion " + opinion + " is correlated to city '" + city + "' (" + percent + "%)"; 
					batch.add(info);
					this.source.sendBatch(batch);
				}
			}
		}
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		logger.fine("Cleaning up OpinionAnalyzer " + serialVersionUID + "...");
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
