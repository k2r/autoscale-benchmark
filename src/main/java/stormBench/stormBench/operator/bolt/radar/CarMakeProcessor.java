/**
 * 
 */
package stormBench.stormBench.operator.bolt.radar;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import stormBench.stormBench.rmi.RMIInfoSource;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.Utils;

/**
 * @author Roland
 *
 */
public class CarMakeProcessor implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2235146329232662541L;
	
	private OutputCollector collector;
	private HashMap<String, Integer> counts;
	private Integer port;
	private RMIInfoSource source;
	
	public static Logger logger = Logger.getLogger("CarMakeProcessor");

	
	public CarMakeProcessor(Integer port){
		this.port = port;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<>();
		try {
			this.source = new RMIInfoSource(port);
		} catch (RemoteException e) {
			System.out.println("Unable to initialize the rmi source because " + e);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String make = input.getStringByField(FieldNames.MAKE.toString());
		if(!this.counts.containsKey(make)){
			this.counts.put(make, 0);
		}
		Integer count = this.counts.get(make);
		count++;
		this.counts.put(make, count);
		this.source.setInfo(Utils.convertToInfo(this.counts));
		this.source.cast();
		this.collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		try {
			this.source.releaseRegistry();
		} catch (RemoteException e) {
			logger.severe("Unable to release the stream source because " + e);
		}
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
