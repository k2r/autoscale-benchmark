package stormBench.stormBench.operator.spout.opinion;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import core.element.IElement;
import core.network.rmi.source.IRMIStreamSource;
import stormBench.stormBench.utils.FieldNames;

public class StreamSimSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 166861228570505700L;
	private static Logger logger = Logger.getLogger("StreamSimSpout");
	private String host;
	private int port;
	private int counter;
	private HashMap<Integer, IElement> inputQueue;
	private Integer sendIndex;
	private Integer receiveIndex;
	private SpoutOutputCollector collector;
	
	/**
	 * 
	 */
	public StreamSimSpout(String host, int port) {
		this.host = host;
		this.port = port;
		this.counter = 0;
		this.inputQueue = new HashMap<>();
		this.sendIndex = 0;
		this.receiveIndex = 0;
	}
	
	public IElement[] getInputStream(){
		IElement[] input = new IElement[0];
		try {
			Registry registry = LocateRegistry.getRegistry(host, port);
			if(registry != null){
				String[] resources = registry.list();
				int n = resources.length;
				for(int i = 0; i < n; i++){
					if(resources[i].equalsIgnoreCase("tuples")){
						IRMIStreamSource stub = (IRMIStreamSource) registry.lookup("tuples");
						int chunkCounter = stub.getChunkCounter();
						if(chunkCounter > this.counter){
							this.counter++;
							input = stub.getInputStream();
						}
						break;
					}
				}
			}
		}catch(Exception e){
			logger.severe("Client exception: " + e.toString());
			e.printStackTrace();
		}
		return input;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {
		logger.info("StreamSimSpout " + StreamSimSpout.serialVersionUID + " is being closed.");
	}

	@Override
	public void activate() {
		logger.info("StreamSimSpout " + StreamSimSpout.serialVersionUID + " is being activated. Listening to host " + this.host + " port " + this.port + "...");
	}

	@Override
	public void deactivate() {
		logger.info("StreamSimSpout " + StreamSimSpout.serialVersionUID + " is being deactivated.");
	}

	@Override
	public void nextTuple() {
		IElement[] input = this.getInputStream();
		int nbElements = input.length;
		if(nbElements > 0){
			for(int i = 0; i < nbElements; i++){
				this.inputQueue.put(receiveIndex, input[i]);
				receiveIndex++;
			}
		}
		while(this.receiveIndex > this.sendIndex){
			IElement element = this.inputQueue.get(this.sendIndex);
			Object[] values = element.getValues();
			String name = (String) values[0];
			Integer age = (Integer) values[1];
			String city = (String) values[2];
			String opinion = (String) values[3];
			
			this.collector.emit(new Values(name, age, city, opinion), this.sendIndex);
			this.sendIndex++;
		}
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			logger.severe("StreamSimSpout can not sleep because of " + e);
		}
	}

	@Override
	public void ack(Object msgId) {
		Integer id  = (Integer) msgId;
		this.inputQueue.remove(id);
		logger.fine("StreamSimSpout " + StreamSimSpout.serialVersionUID + " acked tuple " + id + ".");
	}

	@Override
	public void fail(Object msgId) {
		Integer id  = (Integer) msgId;
		IElement element = this.inputQueue.get(id);
		Object[] values = element.getValues();
		String name = (String) values[0];
		Integer age = (Integer) values[1];
		String city = (String) values[2];
		String opinion = (String) values[3];
		
		this.collector.emit(new Values(name, age, city, opinion), id);
		logger.fine("StreamSimSpout " + StreamSimSpout.serialVersionUID + " failed tuple " + id + ". It has been sent again.");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.NAME.toString(), FieldNames.AGE.toString(), FieldNames.CITY.toString(), FieldNames.OPINION.toString()));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
