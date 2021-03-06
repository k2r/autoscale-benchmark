/**
 * 
 */
package stormBench.stormBench.operator.bolt.opinion;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.Utils;

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
	
	private HashSet<String> ages;//contains all known age categories 
	private HashSet<String> opinions;//contains all known opinions
	private HashMap<Integer, HashMap<String, String>> historic;//rank -> opinion -> age category
	private Integer sampleSize;
	private Integer index;
	
	public AgeAnalyzer(Integer sampleSize) {
		this.sampleSize = sampleSize;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.ages = new HashSet<>();
		this.opinions = new HashSet<>();
		this.historic = new HashMap<>();
		this.index = 0;
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String category = input.getStringByField(FieldNames.CATAGE.toString());
		String opinion = input.getStringByField(FieldNames.OPINION.toString());
		
		/*if the sample is not significant so it is enriched with the new tuple*/
		if(!this.ages.contains(category)){
			this.ages.add(category);
		}
		if(!this.opinions.contains(opinion)){
			this.opinions.add(opinion);
		}
		
		HashMap<String, String> opinionMap = new HashMap<>();
		opinionMap.put(opinion, category);
		Set<Integer> ranks = this.historic.keySet();
		
		if(this.index >= this.sampleSize){
			this.index--;//the index is set to the last valid position
			this.historic.remove(new Integer(0));//discard of the oldest record
			
			HashMap<Integer, HashMap<String, String>> update = new HashMap<>();
			for(Integer rank : ranks){
				opinionMap = this.historic.get(rank);
				update.put(rank - 1, opinionMap);
			}
			opinionMap.put(opinion, category);
			update.put(this.index, opinionMap);//add of the newest record
			this.historic = update;
		}else{
			this.historic.put(this.index, opinionMap);	
		}
		this.index++;
		
		HashMap<String, HashMap<String, Double>> confidences = new HashMap<>();//opinion -> category -> confidence
		/*Computation of confidences for each association rule of the form category->opinion*/
		for(String knownOpinion : this.opinions){
			for(String knownCategory : this.ages){
				Double countCategory = 0.0;
				Double countInter = 0.0;
				for(Integer rank : ranks){
					HashMap<String, String> record = this.historic.get(rank);
					if(record.containsValue(knownCategory)){
						countCategory++;
						if(record.containsKey(knownOpinion)){
							countInter++;
						}
					}
				}
				Double confidence = countInter / countCategory;
				HashMap<String, Double> categoryConfidence = new HashMap<>();
				if(confidences.containsKey(knownOpinion)){//add the confidence for the current association rule in the map
					categoryConfidence = confidences.get(knownOpinion);
				}
				categoryConfidence.put(knownCategory, confidence);
				confidences.put(knownOpinion, categoryConfidence);
			}
		}
		for(String knownOpinion : this.opinions){//selection and emission of the best association rule for opinion according to confidence
			HashMap<String, Double> confidenceMap = confidences.get(knownOpinion);
			String bestCategory = Utils.getMaxCategory(confidenceMap);
			Double bestConfidence = confidenceMap.get(bestCategory);
			this.collector.emit(FieldNames.CATAGE.toString(), input, new Values(bestCategory, knownOpinion, bestConfidence));
		}
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
		declarer.declareStream(FieldNames.CATAGE.toString(), new Fields(FieldNames.CATAGE.toString(), FieldNames.OPINION.toString(), FieldNames.CONFIDENCE.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
