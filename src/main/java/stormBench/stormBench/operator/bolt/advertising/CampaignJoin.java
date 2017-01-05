/**
 * 
 */
package stormBench.stormBench.operator.bolt.advertising;

import java.util.ArrayList;
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
public class CampaignJoin implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2051315972010187684L;
	private OutputCollector collector;
	private HashMap<Integer, ArrayList<Integer>> campaigns;
	
	public CampaignJoin() {
		this.campaigns = new HashMap<>();
		ArrayList<Integer> campaign1 = new ArrayList<>();
		for(int i = 0; i < 1000; i++){
			campaign1.add(i);
		}
		ArrayList<Integer> campaign2 = new ArrayList<>();
		for(int i = 1000; i < 2000; i++){
			campaign2.add(i);
		}
		ArrayList<Integer> campaign3 = new ArrayList<>();
		for(int i = 2000; i < 3000; i++){
			campaign3.add(i);
		}
		campaigns.put(1, campaign1);
		campaigns.put(2, campaign2);
		campaigns.put(3, campaign3);
	}
	
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
		Integer adID = input.getIntegerByField(FieldNames.ADID.toString());
		Integer eventTime = input.getIntegerByField(FieldNames.EVTTIME.toString());
		boolean found = false;
		for(Integer campaignID : campaigns.keySet()){
			ArrayList<Integer> ads = campaigns.get(campaignID);
			if(ads.contains(adID)){
				this.collector.emit(new Values(campaignID, adID, eventTime));
				found = true;
				break;
			}
			if(found){
				break;
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
		declarer.declare(new Fields(FieldNames.CAMPID.toString(), FieldNames.ADID.toString(), FieldNames.EVTTIME.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
