/**
 * 
 */
package stormBench.stormBench.operator.spout;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.zookeeper.ZookeeperClient;

/**
 * @author Roland
 *
 */
public class SyntheticStreamSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2853429592252435680L;
	private static Logger logger = Logger.getLogger("SyntheticStreamSpout");
	private SpoutOutputCollector collector;
	private int index;
	private String stateHost;
	private ZookeeperClient zkClient;
	private HashMap<Integer, String> replayQueue;
	
	private ArrayList<Integer> codes = new ArrayList<>();
	private String reference;
	
	public SyntheticStreamSpout(String stateHost, ArrayList<Integer> codes) {
		this.stateHost = stateHost;
		this.codes = codes;
		this.reference = null;
	}
	
	public SyntheticStreamSpout(String stateHost, ArrayList<Integer> codes, String reference) {
		this.stateHost = stateHost;
		this.codes = codes;
		this.reference = reference;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#open(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.spout.SpoutOutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		String id = context.getStormId() + "_" +  context.getThisComponentId();
		this.collector = collector;
		this.replayQueue = new HashMap<>();
		this.zkClient = new ZookeeperClient(this.stateHost, id);
		try {
			if(this.zkClient.existsZNodeState() != null){
				byte[] rawState = this.zkClient.getState();
				if(rawState != null){
					Integer state = Integer.parseInt(new String(this.zkClient.getState(), Charset.defaultCharset().name()));
					this.index = state;
					System.out.println("Index " + this.index + " successfully retrieved from zNode");
				}else{
					this.index = 0;
				}
			}else{
				this.zkClient.createZNodeState();
				this.index = 0;
			}
			if(this.zkClient.existsZNodeDate() == null){
				this.zkClient.createZNodeDate();
			}
			byte[] rawDate = new Long(System.currentTimeMillis()).toString().getBytes();
			this.zkClient.persistDate(rawDate);
			System.out.println("Emission date successfully reinitialized on zNode");
		} catch (NumberFormatException | UnsupportedEncodingException e) {
			logger.severe("Unable to decode the current state");
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		logger.fine("The increasing stream spout is shutting down....");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
		logger.fine("The increasing stream spout is starting....");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		logger.fine("The increasing stream spout is being deactivated....");
	}

	public String generateTuple(){
		int size = this.codes.size();
		int codeIndex = this.index % size; 
		int code =  this.codes.get(codeIndex);
		String streamId = "";
		switch(code){
		case(0): 	streamId = FieldNames.LYON.toString();
		break;
		case(1): 	streamId = FieldNames.VILLEUR.toString();
		break;
		case(2):	streamId = FieldNames.VAULX.toString();
		break;
		}
		return streamId;
	}
	
	public void emitNewTuple(){
		String streamId = generateTuple();
		if(this.reference != null){
			if(streamId.equalsIgnoreCase(this.reference)){
				this.collector.emit(streamId, new Values(35), this.index);
			}
		}else{
			this.collector.emit(streamId, new Values(35), this.index);
		}
		this.replayQueue.put(this.index, streamId);
		this.index++;
		String state = this.index + "";
		String date = new Long(System.currentTimeMillis()).toString();
		this.zkClient.persistState(state.getBytes());
		this.zkClient.persistDate(date.getBytes());
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		if(this.index < 18000){
			Long lastEmission = Long.parseLong(new String(this.zkClient.getDate()));
			Long now = System.currentTimeMillis();
			Long interval = now - lastEmission;
			if(this.index < 600 && interval >= 250){
				emitNewTuple();
			}
			if(this.index >= 600 && this.index < 1800 && interval >= 100){
				emitNewTuple();
			}
			if(this.index >= 1800 && this.index < 3000 && interval >= 50){
				emitNewTuple();
			}
			if(this.index >= 3000 && this.index < 6000 && interval >= 20){
				emitNewTuple();
			}
			if(this.index >= 6000 && this.index < 12000 && interval >= 1){
				emitNewTuple();
			}
			if(this.index >= 12000 && this.index < 15000 && interval >= 20){
				emitNewTuple();
			}
			if(this.index >= 15000 && this.index < 16200 && interval >= 50){
				emitNewTuple();
			}
			if(this.index >= 16200 && this.index < 18000 && interval >= 250){
				emitNewTuple();
			}
		}else{
			System.out.println("End of test stream!");
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
		Integer id = (Integer) msgId;
		this.replayQueue.remove(id);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#fail(java.lang.Object)
	 */
	@Override
	public void fail(Object msgId) {
		Integer id = (Integer) msgId;
		String streamId = this.replayQueue.get(id);
		this.collector.emit(streamId, new Values(35), id);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(FieldNames.LYON.toString(), new Fields(FieldNames.TEMPERATURE.toString()));
		declarer.declareStream(FieldNames.VILLEUR.toString(), new Fields(FieldNames.TEMPERATURE.toString()));
		declarer.declareStream(FieldNames.VAULX.toString(), new Fields(FieldNames.TEMPERATURE.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}