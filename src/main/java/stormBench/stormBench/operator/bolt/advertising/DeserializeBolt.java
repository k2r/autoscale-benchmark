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
public class DeserializeBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1314812838594234494L;
	private OutputCollector collector;
	
	public DeserializeBolt() {
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
		String log = input.getStringByField(FieldNames.LOGS.toString());
		String[] fields = log.split(";");
		Integer userID = Integer.parseInt(fields[0]);
		Integer pageID = Integer.parseInt(fields[1]);
		Integer adID = Integer.parseInt(fields[2]);
		String adType = fields[3];
		String eventType = fields[4];
		Integer eventTime = Integer.parseInt(fields[5]);
		String ipAddress = fields[6];
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.collector.emit(new Values(userID, pageID, adID, adType, eventType, eventTime, ipAddress));
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
