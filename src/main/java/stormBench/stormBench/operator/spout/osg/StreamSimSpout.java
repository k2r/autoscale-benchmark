/**
 * 
 */
package stormBench.stormBench.operator.spout.osg;

import java.rmi.RemoteException;
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
import core.element.relational.RelationalStreamElement;
import core.network.rmi.consumer.IRMIStreamConsumer;
import core.network.rmi.consumer.RMIStreamConsumer;
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
	 * @return the last set of tuples sent by the stream source
	 */
	public IElement[] getInputStream(){
		IRMIStreamConsumer consumer;
		IElement[] input = new IElement[0];
		try {
			consumer = new RMIStreamConsumer(this.host, this.port, "tuples");
			input = consumer.consume();
		} catch (RemoteException e) {
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
			RelationalStreamElement element = (RelationalStreamElement) this.inputQueue.get(this.sendIndex);
			Object[] values = element.getStreamElement();
			Integer value = (Integer) values[0];
			this.collector.emit(new Values(value), this.sendIndex);
			this.sendIndex++;
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
		RelationalStreamElement element = (RelationalStreamElement) this.inputQueue.get(this.sendIndex);
		Object[] values = element.getStreamElement();
		Integer value = (Integer) values[0];
		this.collector.emit(new Values(value), id);
		logger.fine("StreamSimSpout " + StreamSimSpout.serialVersionUID + " failed tuple " + id + ". It has been sent again.");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.VALUE.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}