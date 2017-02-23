/**
 * 
 */
package stormBench.stormBench.operator.bolt.test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * @author Roland
 *
 */
public class PrimeFactorBolt implements IRichBolt {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3246518949358489427L;
	private String filename;
	private ArrayList<String> buffer;
	private Integer buffIndex;
	private OutputCollector collector;
	private static Logger logger = Logger.getLogger("PrimeFactorBolt");
	
	public PrimeFactorBolt(String filename) {
		this.filename = filename;
		this.buffer = new ArrayList<>();
		this.buffIndex = 0;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		Path path = Paths.get(this.filename);
		try {
			Files.deleteIfExists(path);
			Files.createFile(path);
		} catch (IOException e) {
			logger.severe("Unable to find or write in the file " + this.filename + " because " + e);
		}
	}

	public void flushBuffer(){
		Path path = Paths.get(this.filename);
		try {
			Files.write(path, this.buffer, Charset.defaultCharset(), StandardOpenOption.APPEND);
			this.buffer = new ArrayList<>();
			this.buffIndex = 0;
		} catch (IOException e) {
			logger.severe("Unable to write the buffer because " + e);
		}
	}
	
	public ArrayList<Integer> getPrimeFactors(Integer value){
		ArrayList<Integer> results = new ArrayList<>();
		if(value >= 1){
			for(int i = 2; i <= value; i++){
				while(value % i == 0){
					results.add(i);
					value /= i;
				}
			}
		}
		return results;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		Integer value = input.getIntegerByField("value");
		ArrayList<Integer> primeFactors = getPrimeFactors(value);
		Integer nbFactors = primeFactors.size();
		if(!primeFactors.isEmpty()){
			String toBuff = value + " = ";
			for(int i = 0; i < nbFactors - 1; i++){
				toBuff += primeFactors.get(i) + " x ";
			}
			toBuff += primeFactors.get(nbFactors - 1);
			this.buffer.add(toBuff);
			this.buffIndex++;
		}
		this.collector.ack(input);
		if(this.buffIndex > 10){
			this.flushBuffer();
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
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
