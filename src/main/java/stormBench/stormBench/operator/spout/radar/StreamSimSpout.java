/**
 * 
 */
package stormBench.stormBench.operator.spout.radar;

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

/**
 * @author Roland
 *
 */

public class StreamSimSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -299357684149329360L;
	private static Logger logger = Logger.getLogger("ElementSpoutLogger");
	private String host;
	private int port;
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
		this.inputQueue = new HashMap<>();
		this.sendIndex = 0;
		this.receiveIndex = 0;
	}
	
	/**
	 * Each instance IElement should be cast further into IElement1, IElement2, IElement3 or IElement4 according to the number 
	 * of attributes describing each tuple. It allows to access attribute values through functions getFirstValue() to getFourthValue()
	 * @return the last set of tuples sent by the stream source
	 */
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
						input = stub.getInputStream();
						registry.unbind("tuples");
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
	
	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#open(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.spout.SpoutOutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		logger.info("StreamSimSpout " + StreamSimSpout.serialVersionUID + " is being closed.");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
		logger.info("StreamSimSpout " + StreamSimSpout.serialVersionUID + " is being activated.");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		logger.info("StreamSimSpout " + StreamSimSpout.serialVersionUID + " is being deactivated.");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		IElement[] input = this.getInputStream();
		int nbElements = input.length;
		if(nbElements > 0){
			for(int i = 0; i < nbElements; i++){
				this.inputQueue.put(receiveIndex, input[i]);
				receiveIndex++;
			}
		}else{
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				logger.severe("StreamSimSpout can not sleep because of " + e);
			}
		}
		if(this.receiveIndex > this.sendIndex){
			IElement element = this.inputQueue.get(this.sendIndex);
			Object[] values = element.getValues();
			String registration = (String) values[0];
			Integer speed = (Integer) values[1];
			String make = (String) values[2];
			String color = (String) values[3];
			String location = (String) values[4];
			
			this.collector.emit(new Values(registration, speed, make, color, location), this.sendIndex);
			this.sendIndex++;
			//logger.info("StreamSimSpout info, tuples received: " + receiveIndex + ", tuples pending: " + this.inputQueue.size() + ", tuples transmitted: " + sendIndex);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
		Integer id  = (Integer) msgId;
		this.inputQueue.remove(id);
		logger.fine("StreamSimSpout " + StreamSimSpout.serialVersionUID + " acked tuple " + id + ".");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#fail(java.lang.Object)
	 */
	@Override
	public void fail(Object msgId) {
		Integer id  = (Integer) msgId;
		IElement element = this.inputQueue.get(id);
		Object[] values = element.getValues();
		String registration = (String) values[0];
		Integer speed = (Integer) values[1];
		String make = (String) values[2];
		String color = (String) values[3];
		String location = (String) values[4];
		
		this.collector.emit(new Values(registration, speed, make, color, location), id);
		logger.fine("StreamSimSpout " + StreamSimSpout.serialVersionUID + " failed tuple " + id + ". It has been sent again.");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.REGISTR.toString(), FieldNames.SPEED.toString(), FieldNames.MAKE.toString(), FieldNames.COLOR.toString(), FieldNames.LOC.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}