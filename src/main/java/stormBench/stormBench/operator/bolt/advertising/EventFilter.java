/**
 * 
 */
package stormBench.stormBench.operator.bolt.advertising;

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
public class EventFilter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1510915526299972969L;
	private OutputCollector collector;
	
	public EventFilter() {
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
		String eventType = input.getStringByField(FieldNames.EVTTYPE.toString());
		
		if(eventType.equalsIgnoreCase("view")){
			Integer userID = input.getIntegerByField(FieldNames.USERID.toString());
			Integer pageID = input.getIntegerByField(FieldNames.PGID.toString());
			Integer adID = input.getIntegerByField(FieldNames.ADID.toString());
			String adType = input.getStringByField(FieldNames.ADTYPE.toString());	
			Integer eventTime = input.getIntegerByField(FieldNames.EVTTIME.toString());
			String ipAddress = input.getStringByField(FieldNames.IP.toString());
			
			this.collector.emit(new Values(userID, pageID, adID, adType, eventType, eventTime, ipAddress));
		}
		try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
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
		declarer.declare(new Fields(FieldNames.USERID.toString(), FieldNames.PGID.toString(), FieldNames.ADID.toString(), FieldNames.ADTYPE.toString(), FieldNames.EVTTYPE.toString(), FieldNames.EVTTIME.toString(), FieldNames.IP.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
