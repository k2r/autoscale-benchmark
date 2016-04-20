/**
 * 
 */
package stormBench.stormBench.diamond.bolt;

import java.util.Map;
import java.util.logging.Logger;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import stormBench.stormBench.utils.FieldNames;

/**
 * @author Roland
 *
 */
public class HeatwaveBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9130053201772106746L;
	private static Logger logger = Logger.getLogger("HeatwaveBolt");;
	private OutputCollector collector;
	private int refValue;
	private String city;
	private int zipCode;
	private double latitude;
	private double longitude;
	
	public HeatwaveBolt(String city, int refValue) {
		this.city = city;
		this.refValue = refValue;
		if(city.equalsIgnoreCase(FieldNames.LYON.toString())){
			this.zipCode = 69000;
			this.latitude = 45.770748;
			this.longitude = 4.847822;
		}
		if(city.equalsIgnoreCase(FieldNames.VILLEUR.toString())){
			this.zipCode = 69000;
			this.latitude = 45.777932; 
			this.longitude = 4.881468;
		}
		if(city.equalsIgnoreCase(FieldNames.VAULX.toString())){
			this.zipCode = 69000;
			this.latitude = 45.788227;
			this.longitude = 4.928159;
		}
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields(FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		HeatwaveBolt.logger.info("HeatwaveBolt " + HeatwaveBolt.serialVersionUID + " is going to shutdown");
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple arg0) {
		int temperature = arg0.getIntegerByField(FieldNames.TEMPERATURE.toString());
		if(temperature > this.refValue){
			collector.emit(arg0, new Values(this.city, this.zipCode, this.latitude, this.longitude, temperature));
			collector.ack(arg0);
		}
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
	}

}
