/**
 * 
 */
package stormBench.stormBench.operator.spout;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
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
import core.element.IElement;
import core.element.element2.IElement2;
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
	
	/**
	 * 
	 * @return the list of attribute names
	 */
	public ArrayList<String> getAttrNames(){
		ArrayList<String> input = new ArrayList<>();
		try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            if(registry != null){
            	IRMIStreamSource stub = (IRMIStreamSource) registry.lookup("tuples");
				input = stub.getAttrNames();
				registry.unbind("tuples");
            }
		}catch(Exception e){
			logger.severe("Client exception: " + e.toString());
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
	@SuppressWarnings("rawtypes")
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
			IElement2 element = (IElement2) this.inputQueue.get(this.sendIndex);
			Integer temperature = (Integer) element.getFirstValue();
			Integer code = (Integer) element.getSecondValue();
			String streamId = null;
			switch(code){
			case(1): 	streamId = FieldNames.LYON.toString();
			break;
			case(2): 	streamId = FieldNames.VILLEUR.toString();
			break;
			case(3):	streamId = FieldNames.VAULX.toString();
			break;
			}
			this.collector.emit(streamId, new Values(temperature), this.sendIndex);
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
	@SuppressWarnings("rawtypes")
	@Override
	public void fail(Object msgId) {
		Integer id  = (Integer) msgId;
		IElement2 element = (IElement2) this.inputQueue.get(id);
		Integer temperature = (Integer) element.getFirstValue();
		Integer code = (Integer) element.getSecondValue();
		String streamId = null;
		switch(code){
		case(1): 	streamId = FieldNames.LYON.toString();
		break;
		case(2): 	streamId = FieldNames.VILLEUR.toString();
		break;
		case(3):	streamId = FieldNames.VAULX.toString();
		break;
		}
		this.collector.emit(streamId, new Values(temperature), id);
		logger.fine("StreamSimSpout " + StreamSimSpout.serialVersionUID + " failed tuple " + id + ". It has been sent again.");
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