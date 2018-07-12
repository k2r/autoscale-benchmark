/**
 * 
 */
package stormBench.stormBench.operator.spout.advertising;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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
public class AdvertisingStreamSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2853429592252435680L;
	private static Logger logger = Logger.getLogger("AdvertisingStreamSpout");
	private SpoutOutputCollector collector;
	private int index;
	private String stateHost;
	private ZookeeperClient zkClient;
	private HashMap<Integer, String> replayQueue;
	
	
	public AdvertisingStreamSpout(String stateHost) {
		this.stateHost = stateHost;
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
		logger.fine("The advertising stream spout is shutting down....");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
		logger.fine("The advertising stream spout is starting....");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		logger.fine("The advertising stream spout is being deactivated....");
	}

	public String generateTuple(){
		String log = "";
		Random random = new Random();
		
		Integer userID = random.nextInt(1000);
		Integer pageID = random.nextInt(200);
		Integer adID = random.nextInt(3000);
		
		ArrayList<String> adTypes = new ArrayList<>();
		adTypes.add("image");
		adTypes.add("video");
		adTypes.add("header");
		adTypes.add("footer");
		Integer adIndex = random.nextInt(4);
		String adType = adTypes.get(adIndex);
		
		ArrayList<String> eventTypes = new ArrayList<>();
		eventTypes.add("view");
		eventTypes.add("click");
		eventTypes.add("hover");
		Integer eventIndex = random.nextInt(3);
		String eventType = eventTypes.get(eventIndex);
		
		Integer eventTime = random.nextInt(10000);
		
		String ipAddress = "192.168.";
		Integer ipComplement1 = random.nextInt(80);
		Integer ipComplement2 = random.nextInt(150) + 50;
		ipAddress += ipComplement1 + "." + ipComplement2;
		
		
		log += userID + ";" + pageID + ";" + adID + ";" + adType + ";" + eventType + ";" + eventTime + ";" + ipAddress + ";";
		return log;
	}
	
	public void emitNewTuple(){
		String streamId = FieldNames.LOGS.toString();
		String log = generateTuple();
		this.collector.emit(streamId, new Values(log), this.index);
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
		if(this.index < 10000){
			Long lastEmission = Long.parseLong(new String(this.zkClient.getDate()));
			Long now = System.currentTimeMillis();
			Long interval = now - lastEmission;
			if(this.index < 100 && interval >= 250){
				emitNewTuple();
			}
			if(this.index >= 100 && this.index < 1100 && interval >= 1){
				emitNewTuple();
			}
			if(this.index >= 1100 && this.index < 5000 && interval >= 10){
				emitNewTuple();
			}
			if(this.index >= 5000 && this.index < 7000 && interval >= 20){
				emitNewTuple();
			}
			if(this.index >= 7000 && this.index < 10000 && interval >= 1){
				emitNewTuple();
			}
			if(this.index >= 10000 && this.index < 10200 && interval >= 100){
				emitNewTuple();
			}
			if(this.index >= 10200 && this.index < 13000 && interval >= 5){
				emitNewTuple();
			}
			if(this.index >= 13000 && this.index < 13500 && interval >= 10){
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
		declarer.declareStream(FieldNames.LOGS.toString(), new Fields(FieldNames.LOGS.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}