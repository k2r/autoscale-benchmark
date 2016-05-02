package stormBench.stormBench.operator.bolt;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import stormBench.stormBench.hook.BenchHook;
import stormBench.stormBench.utils.FieldNames;

public class StarHeatwaveBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2316929064298169656L;
	private static Logger logger = Logger.getLogger("StarHeatwaveBolt");
	private OutputCollector collector;
	private String dbHost;
	
	private static final String city = "city";
	private static final String refValue = "refValue";
	private static final String zipCode = "zipCode";
	private static final String latitude = "latitude";
	private static final String longitude = "longitude";
	
	private HashMap<String, String> lyon;
	private HashMap<String, String> villeurbanne;
	private HashMap<String, String> vaulx;
	
	public StarHeatwaveBolt() {
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
	
	public StarHeatwaveBolt(String dbHost){
		this();
		this.dbHost = dbHost;
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declareStream(FieldNames.LYON.toString(), new Fields(FieldNames.ID.toString(), FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
		arg0.declareStream(FieldNames.VILLEUR.toString(), new Fields(FieldNames.ID.toString(), FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
		arg0.declareStream(FieldNames.VAULX.toString(), new Fields(FieldNames.ID.toString(), FieldNames.CITY.toString(), FieldNames.ZIP.toString(), FieldNames.LAT.toString(), FieldNames.LONGIT.toString(), FieldNames.TEMPERATURE.toString()));
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
		StarHeatwaveBolt.logger.info("StarHeatwaveBolt " + StarHeatwaveBolt.serialVersionUID + " is going to shutdown");
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple arg0) {
		int temperature = arg0.getIntegerByField(FieldNames.TEMPERATURE.toString());
		String streamId = arg0.getSourceStreamId();
		if(streamId.equalsIgnoreCase(FieldNames.LYON.toString())){
			if(temperature > Integer.parseInt(this.lyon.get(refValue))){
				collector.emit(FieldNames.LYON.toString(), arg0, new Values(0, this.lyon.get(city), Integer.parseInt(this.lyon.get(zipCode)), 
						Double.parseDouble(this.lyon.get(latitude)), Double.parseDouble(this.lyon.get(longitude)),
						temperature));
				collector.ack(arg0);
			}else{
				collector.ack(arg0);
			}
		}
		if(streamId.equalsIgnoreCase(FieldNames.VILLEUR.toString())){
			if(temperature > Integer.parseInt(this.villeurbanne.get(refValue))){
				collector.emit(FieldNames.VILLEUR.toString(), arg0, new Values(0, this.villeurbanne.get(city), Integer.parseInt(this.villeurbanne.get(zipCode)), 
						Double.parseDouble(this.villeurbanne.get(latitude)), Double.parseDouble(this.villeurbanne.get(longitude)),
						temperature));
				collector.ack(arg0);
			}else{
				collector.ack(arg0);
			}
		}
		if(streamId.equalsIgnoreCase(FieldNames.VAULX.toString())){
			if(temperature > Integer.parseInt(this.vaulx.get(refValue))){
				collector.emit(FieldNames.VAULX.toString(), arg0, new Values(0, this.vaulx.get(city), Integer.parseInt(this.vaulx.get(zipCode)), 
						Double.parseDouble(this.vaulx.get(latitude)), Double.parseDouble(this.vaulx.get(longitude)),
						temperature));
				collector.ack(arg0);
			}else{
				collector.ack(arg0);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			context.addTaskHook(new BenchHook(this.dbHost));
		} catch (ClassNotFoundException e) {
			logger.warning("Hook can not be attached to ElementSpout " + StarHeatwaveBolt.serialVersionUID + " because the JDBC driver can not be found, error: " + e );
		} catch (SQLException e) {
			logger.warning("Hook can not be attached to ElementSpout " + StarHeatwaveBolt.serialVersionUID + " because of invalid JDBC configuration , error: " + e);
		}
	}
}