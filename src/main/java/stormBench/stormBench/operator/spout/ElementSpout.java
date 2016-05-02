/**
 * 
 */
package stormBench.stormBench.operator.spout;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import core.element.IElement;
import core.element.element2.IElement2;
import core.network.rmi.source.IRMIStreamSource;
import stormBench.stormBench.hook.BenchHook;
import stormBench.stormBench.utils.FieldNames;

/**
 * @author Roland
 *
 */
public class ElementSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -299357684149329360L;
	private static Logger logger = Logger.getLogger("ElementSpoutLogger");
	private String host;
	private int port;
	private String dbHost;
	private SpoutOutputCollector collector;
	private int msgId;

	/**
	 * 
	 */
	public ElementSpout(String host, int port) {
		this.host = host;
		this.port = port;
		this.dbHost = null;
	}
	
	public ElementSpout(String host, int port, String dbHost) {
		this(host, port);
		this.dbHost = dbHost;
	}
	
	/**
	 * Each instance IElement should be cast further into IElement1, IElement2, IElement3 or IElement4 according to the number 
	 * of attributes describing each tuple. It allows to access attribute values through functions getFirstValue() to getFourthValue()
	 * @return the last set of tuples sent by the stream source
	 */
	public IElement[] getInputStream(){
		IElement[] input = null;
		try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            if(registry != null){
            	IRMIStreamSource stub = (IRMIStreamSource) registry.lookup("tuples");
				input = stub.getInputStream();
            }
		}catch(Exception e){
			ElementSpout.logger.severe("Client exception: " + e.toString());
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
            }
		}catch(Exception e){
			ElementSpout.logger.severe("Client exception: " + e.toString());
			e.printStackTrace();
		}
		return input;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.msgId = 0;
		try {
			context.addTaskHook(new BenchHook(this.dbHost));
		} catch (ClassNotFoundException e) {
			logger.warning("Hook can not be attached to ElementSpout " + ElementSpout.serialVersionUID + " because the JDBC driver can not be found, error: " + e );
		} catch (SQLException e) {
			logger.warning("Hook can not be attached to ElementSpout " + ElementSpout.serialVersionUID + " because of invalid JDBC configuration , error: " + e);
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		ElementSpout.logger.info("ElementSpout " + ElementSpout.serialVersionUID + " is being closed.");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
		ElementSpout.logger.info("ElementSpout " + ElementSpout.serialVersionUID + " is being activated.");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		ElementSpout.logger.info("ElementSpout " + ElementSpout.serialVersionUID + " is being deactivated.");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void nextTuple() {
		IElement[] input = this.getInputStream();
		int nbElements = input.length;
		for(int i = 0; i < nbElements; i++){
			IElement2 element = (IElement2) input[i];
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
			this.collector.emit(streamId, new Values(temperature), this.msgId);
			this.msgId++;
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
		Integer id  = (Integer) msgId;
		ElementSpout.logger.fine("ElementSpout " + ElementSpout.serialVersionUID + " acked tuple " + id + ".");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
	 */
	@Override
	public void fail(Object msgId) {
		Integer id  = (Integer) msgId;
		ElementSpout.logger.fine("ElementSpout " + ElementSpout.serialVersionUID + " failed tuple " + id + ".");
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
