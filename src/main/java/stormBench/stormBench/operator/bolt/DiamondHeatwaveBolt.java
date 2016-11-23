/**
 * 
 */
package stormBench.stormBench.operator.bolt;

import java.util.Map;
import java.util.logging.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormBench.stormBench.utils.FieldNames;

/**
 * @author Roland
 *
 */
public class DiamondHeatwaveBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9130053201772106746L;
	private static Logger logger = Logger.getLogger("DiamondHeatwaveBolt");;
	private OutputCollector collector;
	private int refValue;
	private String city;
	private int zipCode;
	private double latitude;
	private double longitude;
	
	public DiamondHeatwaveBolt(String city, int refValue) {
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
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declareStream(this.city, new Fields(FieldNames.ID.toString(), FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		DiamondHeatwaveBolt.logger.info("DiamondHeatwaveBolt " + DiamondHeatwaveBolt.serialVersionUID + " is going to shutdown");
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IRichBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	public void execute(Tuple arg0) {
		try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			logger.severe("Intermediate bolt is unable to sleep because " + e);
		}
		int temperature = arg0.getIntegerByField(FieldNames.TEMPERATURE.toString());
		if(temperature > this.refValue){
			collector.emit(this.city, arg0, new Values(0, this.city, this.zipCode, this.latitude, this.longitude, temperature));
			collector.ack(arg0);
			return;
			
		}else{
			collector.ack(arg0);
			return;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IRichBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

}