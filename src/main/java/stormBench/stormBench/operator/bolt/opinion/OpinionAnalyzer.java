/**
 * 
 */
package stormBench.stormBench.operator.bolt.opinion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import stormBench.stormBench.utils.FieldNames;

/**
 * @author Roland
 *
 */
public class OpinionAnalyzer implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3146352036240971007L;
	private static Logger logger = Logger.getLogger("OpinionAnalyzer");
	private OutputCollector collector;
	
	
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
		String stream = input.getSourceStreamId();
		if(stream.equalsIgnoreCase(FieldNames.CATAGE.toString())){
			String category = input.getStringByField(FieldNames.CATAGE.toString());
			String opinion = input.getStringByField(FieldNames.OPINION.toString());
			Double confidence = input.getDoubleByField(FieldNames.CONFIDENCE.toString());	
			
			String info = "Opinion " + opinion + " is implied by age in " + category + " (confidence = " + confidence + ")";
			Path output = Paths.get("opinion.txt");
			try {
				Files.write(output, Arrays.asList(info), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.severe("Unable to log opinion mine results because " + e);
			}
		}
		if(stream.equalsIgnoreCase(FieldNames.NORMCITY.toString())){
			String city = input.getStringByField(FieldNames.NORMCITY.toString());
			String opinion = input.getStringByField(FieldNames.OPINION.toString());
			Double confidence = input.getDoubleByField(FieldNames.CONFIDENCE.toString());
			
			String info = "Opinion " + opinion + " is implied by city '" + city + "' (confidence = " + confidence + ")"; 
			Path output = Paths.get("opinion.txt");
			try {
				Files.write(output, Arrays.asList(info), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.severe("Unable to log opinion mine results because " + e);
			}
		}
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		logger.fine("Cleaning up OpinionAnalyzer " + serialVersionUID + "...");
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {	
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
