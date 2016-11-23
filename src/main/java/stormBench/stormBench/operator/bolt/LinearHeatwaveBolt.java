package stormBench.stormBench.operator.bolt;

import java.util.HashMap;
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

public class LinearHeatwaveBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4262369370788107343L;
	private static Logger logger = Logger.getLogger("LinearHeatwaveBolt");
	private OutputCollector collector;
	
	private static final String city = "city";
	private static final String refValue = "refValue";
	private static final String zipCode = "zipCode";
	private static final String latitude = "latitude";
	private static final String longitude = "longitude";
	
	private HashMap<String, String> lyon;
	private HashMap<String, String> villeurbanne;
	private HashMap<String, String> vaulx;
	
	public LinearHeatwaveBolt() {
		
		this.lyon = new HashMap<>();
		this.villeurbanne = new HashMap<>();
		this.vaulx = new HashMap<>();
		
		this.lyon.put(city, FieldNames.LYON.toString());
		this.lyon.put(refValue, "28");
		this.lyon.put(zipCode, "69000");
		this.lyon.put(latitude, "45.770748");
		this.lyon.put(longitude, "4.847822");
		
		this.villeurbanne.put(city, FieldNames.VILLEUR.toString());
		this.villeurbanne.put(refValue, "30");
		this.villeurbanne.put(zipCode, "69100");
		this.villeurbanne.put(latitude, "45.777932");
		this.villeurbanne.put(longitude, "4.881468");
		
		this.vaulx.put(city, FieldNames.VAULX.toString());
		this.vaulx.put(refValue, "26");
		this.vaulx.put(zipCode, "69120");
		this.vaulx.put(latitude, "45.788227");
		this.vaulx.put(longitude, "4.928159");
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declareStream(FieldNames.LYON.toString(), new Fields(FieldNames.ID.toString(), FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
		arg0.declareStream(FieldNames.VILLEUR.toString(), new Fields(FieldNames.ID.toString(), FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
		arg0.declareStream(FieldNames.VAULX.toString(), new Fields(FieldNames.ID.toString(), FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
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
		LinearHeatwaveBolt.logger.info("LinearHeatwaveBolt " + LinearHeatwaveBolt.serialVersionUID + " is going to shutdown");
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
		String streamId = arg0.getSourceStreamId();
		if(streamId.equalsIgnoreCase(FieldNames.LYON.toString())){
			if(temperature > Integer.parseInt(this.lyon.get(refValue))){
				collector.emit(FieldNames.LYON.toString(), arg0, new Values(0, this.lyon.get(city), Integer.parseInt(this.lyon.get(zipCode)), 
						Double.parseDouble(this.lyon.get(latitude)), Double.parseDouble(this.lyon.get(longitude)),
						temperature));
				collector.ack(arg0);
				return;
			}else{
				collector.ack(arg0);
				return;
			}
		}
		if(streamId.equalsIgnoreCase(FieldNames.VILLEUR.toString())){
			if(temperature > Integer.parseInt(this.villeurbanne.get(refValue))){
				collector.emit(FieldNames.VILLEUR.toString(), arg0, new Values(0, this.villeurbanne.get(city), Integer.parseInt(this.villeurbanne.get(zipCode)), 
						Double.parseDouble(this.villeurbanne.get(latitude)), Double.parseDouble(this.villeurbanne.get(longitude)),
						temperature));
				collector.ack(arg0);
				return;
			}else{
				collector.ack(arg0);
				return;
			}
		}
		if(streamId.equalsIgnoreCase(FieldNames.VAULX.toString())){
			if(temperature > Integer.parseInt(this.vaulx.get(refValue))){
				collector.emit(FieldNames.VAULX.toString(), arg0, new Values(0, this.vaulx.get(city), Integer.parseInt(this.vaulx.get(zipCode)), 
						Double.parseDouble(this.vaulx.get(latitude)), Double.parseDouble(this.vaulx.get(longitude)),
						temperature));
				collector.ack(arg0);
				return;
			}else{
				collector.ack(arg0);
				return;
			}
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