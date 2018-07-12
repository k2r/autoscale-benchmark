/**
 * 
 */
package stormBench.stormBench.operator.bolt.benchmark;

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
public class CityNormalizerSensitive implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1148955243811355719L;
	private OutputCollector collector;
	private static Logger logger = Logger.getLogger("CityNormalizer");

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String city = input.getStringByField(FieldNames.CITY.toString());
		String normCity = normalizedCity(city);
		String opinion = input.getStringByField(FieldNames.OPINION.toString());
		this.collector.emit(input, new Values(normCity, opinion));
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		logger.fine("Cleaning up CityNormalizer " + serialVersionUID + "...");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.NORMCITY.toString(), FieldNames.OPINION.toString()));
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public String normalizedCity(String code){
		String normCity = "";	
		int procTime = 1;
		if(code.equalsIgnoreCase("NY")){
			normCity += "New York, USA";
			procTime = 10;
		}
		if(code.equalsIgnoreCase("TKY")){
			normCity += "Tokyo, Japan";
			procTime = 20;
		}
		if(code.equalsIgnoreCase("PAR")){
			normCity += "Paris, France";
			procTime = 30;
		}
		if(code.equalsIgnoreCase("BER")){
			normCity += "Berlin, Germany";
			procTime = 40;
		}
		if(code.equalsIgnoreCase("MAD")){
			normCity += "Madrid, Spain";
			procTime = 50;
		}
		if(code.equalsIgnoreCase("LIS")){
			normCity += "Lisboa, Portugal";
			procTime = 60;
		}
		if(code.equalsIgnoreCase("ROM")){
			normCity += "Roma, Italy";
			procTime = 70;
		}
		if(code.equalsIgnoreCase("BRA")){
			normCity += "Brasilia, Brasil";
			procTime = 80;
		}
		if(code.equalsIgnoreCase("SYD")){
			normCity += "Sydney, Australia";
			procTime = 90;
		}
		if(code.equalsIgnoreCase("TAC")){
			normCity += "Tachkent, Ouzbekistan";
			procTime = 100;
		}
	
		try {
			Thread.sleep(procTime);
		} catch (InterruptedException e) {
			logger.severe("CityNormalizer failed to retrieve full city name because " + e);
		}
		return normCity;
	}
}
