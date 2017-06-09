/**
 * 
 */
package stormBench.stormBench.operator.spout.opinion;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
public class SyntheticStreamSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5273393035109120451L;
	private static Logger logger = Logger.getLogger("SyntheticStreamSpout");
	private SpoutOutputCollector collector;
	private int index;
	private String stateHost;
	private ZookeeperClient zkClient;
	private HashMap<Integer, String> replayQueue;
	
	private List<String> cities;
	private List<String> opinions;
	
	public SyntheticStreamSpout(String stateHost) {
		this.stateHost = stateHost;
		this.cities = Arrays.asList("NY", "TKY", "PAR", "BER", "MAD", "TAC", "LIS", "ROM", "BRA", "SYD");
		this.opinions = Arrays.asList("very negative", "negative", "neutral", "positive", "very positive");
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
	
	public void emitNewTuple(){
		Random rand = new Random(49991);//a seed (random great prime number) for reproductibility
		String name = "anonymous_user";
		Integer age = rand.nextInt(60) + 16;
		int nbCities = 10;
		String city = this.cities.get(rand.nextInt(nbCities));
		int nbOpinions = 5;
		String opinion = this.opinions.get(rand.nextInt(nbOpinions));
		this.collector.emit(new Values(name, age, city, opinion), this.index);
		String tupleAsString = name + ";" + age + ";" + city + ";" + opinion;
		this.replayQueue.put(this.index, tupleAsString);
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
		String tupleAsString = this.replayQueue.get(id);
		String[] values = tupleAsString.split(";");
		String name = values[0];
		Integer age = Integer.parseInt(values[1]);
		String city = values[2];
		String opinion = values[3];
		this.collector.emit(new Values(name, age, city, opinion), id);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.NAME.toString(), FieldNames.AGE.toString(), FieldNames.CITY.toString(), FieldNames.OPINION.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
