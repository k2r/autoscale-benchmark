/**
 * 
 */
package stormBench.stormBench.operator.spout;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.zookeeper.ZookeeperClient;

/**
 * @author Roland
 *
 */
public class IncreasingStreamSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2853429592252435680L;
	private static Logger logger = Logger.getLogger("IncreasingStreamSpout");
	private SpoutOutputCollector collector;
	private int index;
	private String stateHost;
	private ZookeeperClient zkClient;
	private HashMap<Integer, String> replayQueue;
	
	public IncreasingStreamSpout(String stateHost) {
		this.stateHost = stateHost;
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		String id = context.getStormId();
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
	 * @see backtype.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		logger.fine("The increasing stream spout is shutting down....");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
		logger.fine("The increasing stream spout is starting....");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		logger.fine("The increasing stream spout is being deactivated....");
	}

	public String generateTuple(){
		int code = this.index % 3;
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
		this.collector.emit(streamId, new Values(35), this.index);
		this.replayQueue.put(this.index, streamId);
		this.index++;
		String state = this.index + "";
		String date = new Long(System.currentTimeMillis()).toString();
		this.zkClient.persistState(state.getBytes());
		this.zkClient.persistDate(date.getBytes());
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		if(this.index < 20000){
			Long lastEmission = Long.parseLong(new String(this.zkClient.getDate()));
			Long now = System.currentTimeMillis();
			Long interval = now - lastEmission;
			if(this.index < 200 && interval >= 500){
				emitNewTuple();
			}
			if(this.index >= 200 && this.index < 600 && interval >= 250){
				emitNewTuple();
			}
			if(this.index >= 600 && this.index < 1200 && interval >= 100){
				emitNewTuple();
			}
			if(this.index >= 1200 && this.index < 3000 && interval >= 50){
				emitNewTuple();
			}
			if(this.index >= 3000 && this.index < 8000 && interval >= 10){
				emitNewTuple();
			}
			if(this.index >= 8000 && this.index < 20000 && interval >= 1){
				emitNewTuple();
			}
		}else{
			System.out.println("End of test stream!");
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
		Integer id = (Integer) msgId;
		this.replayQueue.remove(id);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
	 */
	@Override
	public void fail(Object msgId) {
		Integer id = (Integer) msgId;
		String streamId = this.replayQueue.get(id);
		this.collector.emit(streamId, new Values(35), id);
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(FieldNames.LYON.toString(), new Fields(FieldNames.TEMPERATURE.toString()));
		declarer.declareStream(FieldNames.VILLEUR.toString(), new Fields(FieldNames.TEMPERATURE.toString()));
		declarer.declareStream(FieldNames.VAULX.toString(), new Fields(FieldNames.TEMPERATURE.toString()));
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}