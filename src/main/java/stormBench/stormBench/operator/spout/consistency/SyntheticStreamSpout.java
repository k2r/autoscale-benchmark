/**
 * 
 */
package stormBench.stormBench.operator.spout.consistency;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
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
	private int stream;
	private int index;
	private String stateHost;
	private ZookeeperClient zkClient;
	private HashMap<Integer, String> replayQueue;
	
	private List<String> cities;
	private List<String> opinions;
	private String distribution;
	private Double skew;
	private JDKRandomGenerator generator;
	private ZipfDistribution zipfAge;
	private ZipfDistribution zipfCity;
	private ZipfDistribution zipfOpinion;
	
	public SyntheticStreamSpout(Integer stream, String stateHost, String distribution, Double skew) {
		this.stateHost = stateHost;
		this.stream = stream;
		this.cities = Arrays.asList("NY", "TKY", "PAR", "BER", "MAD", "TAC", "LIS", "ROM", "BRA", "SYD");
		this.opinions = Arrays.asList("very negative", "negative", "neutral", "positive", "very positive");
		this.distribution = distribution;
		this.skew = skew;
		this.generator = new JDKRandomGenerator(49991);//a seed (random great prime number) for reproductibility
		this.zipfAge = new ZipfDistribution(generator, 60, this.skew);
		this.zipfCity = new ZipfDistribution(generator, this.cities.size() - 1, this.skew);
		this.zipfOpinion = new ZipfDistribution(generator, this.opinions.size() -1 , this.skew);
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
			System.out.println("Emission date successfully updated on zNode");
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
		String name = "anonymous_user";
		Integer age =  16;
		String city = "";
		String opinion = "";
		
		if(this.distribution.equalsIgnoreCase("uniform")){
			age += generator.nextInt(60);
			city += this.cities.get(generator.nextInt(this.cities.size()));
			opinion += this.opinions.get(generator.nextInt(this.opinions.size()));
		}
		if(this.distribution.equalsIgnoreCase("zipf")){
			age += zipfAge.sample();
			city += this.cities.get(zipfCity.sample());
			opinion += this.opinions.get(zipfOpinion.sample());
		}
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
		if(this.stream == 1){
			if(this.index < 15000){
				Long lastEmission = Long.parseLong(new String(this.zkClient.getDate()));
				Long now = System.currentTimeMillis();
				Long interval = now - lastEmission;
				if(this.index < 400 && interval >= 250){
					emitNewTuple();
				}
				if(this.index >= 400 && this.index < 1200 && interval >= 100){
					emitNewTuple();
				}
				if(this.index >= 1200 && this.index < 2500 && interval >= 50){
					emitNewTuple();
				}
				if(this.index >= 2500 && this.index < 5000 && interval >= 20){
					emitNewTuple();
				}
				if(this.index >= 5000 && this.index < 10000 && interval >= 5){
					emitNewTuple();
				}
				if(this.index >= 10000 && this.index < 12500 && interval >= 20){
					emitNewTuple();
				}
				if(this.index >= 12500 && this.index < 14000 && interval >= 50){
					emitNewTuple();
				}
				if(this.index >= 14000 && this.index < 15000 && interval >= 250){
					emitNewTuple();
				}
			}else{
				System.out.println("End of test stream!");
			}
		}
		
		if(this.stream == 2){
			if(this.index < 15000){
				Long lastEmission = Long.parseLong(new String(this.zkClient.getDate()));
				Long now = System.currentTimeMillis();
				Long interval = now - lastEmission;
				if(this.index < 400 && interval >= 250){
					emitNewTuple();
				}
				if(this.index >= 400 && this.index < 800 && interval >= 100){
					emitNewTuple();
				}
				if(this.index >= 800 && this.index < 2800 && interval >= 5){
					emitNewTuple();
				}
				if(this.index >= 2800 && this.index < 4800 && interval >= 50){
					emitNewTuple();
				}
				if(this.index >= 4800 && this.index < 6800 && interval >= 5){
					emitNewTuple();
				}
				if(this.index >= 6800 && this.index < 7200 && interval >= 100){
					emitNewTuple();
				}
				if(this.index >= 7200 && this.index < 8200 && interval >= 5){
					emitNewTuple();
				}
				if(this.index >= 8200 && this.index < 8700 && interval >= 250){
					emitNewTuple();
				}
				if(this.index >= 8700 && this.index < 13700 && interval >= 20){
					emitNewTuple();
				}
				if(this.index >= 13700 && this.index < 14500 && interval >= 50){
					emitNewTuple();
				}
				if(this.index >= 14500 && this.index < 15000 && interval >= 250){
					emitNewTuple();
				}
			}else{
				System.out.println("End of test stream!");
			}
		}
		
		if(this.stream == 3){
			if(this.index < 30000){
				Long lastEmission = Long.parseLong(new String(this.zkClient.getDate()));
				Long now = System.currentTimeMillis();
				Long interval = now - lastEmission;
				if(this.index < 30000 && interval >= 50){
					emitNewTuple();
				}
			}else{
				System.out.println("End of test stream!");
			}
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
