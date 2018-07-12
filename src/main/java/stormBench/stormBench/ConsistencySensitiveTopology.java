/**
 * 
 */
package stormBench.stormBench;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.xml.sax.SAXException;

import stormBench.stormBench.operator.bolt.consistency.FastFilter;
import stormBench.stormBench.operator.bolt.consistency.FastNonFilter;
import stormBench.stormBench.operator.bolt.consistency.SlowNonFilter;
import stormBench.stormBench.operator.spout.consistency.SyntheticStreamSpout;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

/**
 * @author Roland
 *
 */
public class ConsistencySensitiveTopology {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws AuthorizationException 
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Integer stream = Integer.parseInt(args[0]);
		String distribution = args[2];
		Double skew = 1.0;
		if(args[3] != null){
			skew = Double.parseDouble(args[3]);
		}
		

		/**
		 * Setting of execution parameters
		 */
		XmlTopologyConfigParser parameters = new XmlTopologyConfigParser("topParameters.xml");
		parameters.initParameters();
		
		String stateHost = parameters.getStateHost();
		String topId = parameters.getTopId();
		
		int nbTasks = Integer.parseInt(parameters.getNbTasks());
		int lightNbExecutors = Integer.parseInt(parameters.getInterNbExecutors());
		int heavyNbExecutors = Integer.parseInt(parameters.getSinkNbExecutors());
		Double lightCpuConstraint = Double.parseDouble(parameters.getInterCpuConstraint());
		Double heavyCpuConstraint = Double.parseDouble(parameters.getSinkCpuConstraint());
		Double lightMemConstraint = Double.parseDouble(parameters.getInterMemConstraint());
		Double heavyMemConstraint = Double.parseDouble(parameters.getSinkMemConstraint());
		Integer nbAckers = Integer.parseInt(parameters.getNbAckers());
		Integer nbWorkers = Integer.parseInt(parameters.getNbWorkers());
		
		/**
         * Declaration of the Consistency checking topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        SyntheticStreamSpout spout = new SyntheticStreamSpout(stream, stateHost, distribution, skew);
        
        builder.setSpout("Source", spout, 1).setCPULoad(20).setMemoryLoad(512);
        
        builder.setBolt("FastNonFilter", new FastNonFilter(),lightNbExecutors)
    	.setCPULoad(lightCpuConstraint).setMemoryLoad(lightMemConstraint).setNumTasks(nbTasks)
    	.shuffleGrouping("Source");

        builder.setBolt("SlowNonFilterMid", new SlowNonFilter(), heavyNbExecutors)
    	.setCPULoad(heavyCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
    	.shuffleGrouping("FastNonFilter");
        
        builder.setBolt("FastFilter", new FastFilter(), lightNbExecutors)
    	.setCPULoad(lightCpuConstraint).setMemoryLoad(lightMemConstraint).setNumTasks(nbTasks)
    	.shuffleGrouping("SlowNonFilterMid");
        
        builder.setBolt("SlowNonFilterEnd", new SlowNonFilter(), heavyNbExecutors)
    	.setCPULoad(heavyCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
    	.shuffleGrouping("FastFilter");
        
        /**
         * Configuration of metadata of the topology
         */
        Config config = new Config();
        config.setNumAckers(nbAckers);
        config.setNumWorkers(nbWorkers);
		
		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology(topId, config, builder.createTopology());
	}

}
