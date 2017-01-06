/**
 * 
 */
package stormBench.stormBench;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormBench.stormBench.operator.bolt.radar.CarMakeProcessor;
import stormBench.stormBench.operator.bolt.radar.CarMakeProjector;
import stormBench.stormBench.operator.bolt.radar.DriverProcessor;
import stormBench.stormBench.operator.bolt.radar.LocationProcessor;
import stormBench.stormBench.operator.bolt.radar.RegistrationProcessor;
import stormBench.stormBench.operator.bolt.radar.SpeedLimitFilter;
import stormBench.stormBench.operator.bolt.radar.ViolationProjector;
import stormBench.stormBench.operator.spout.radar.StreamSimSpout;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

/**
 * @author Roland
 *
 */
public class RadarTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		/**
		 * Setting of execution parameters
		 */
		XmlTopologyConfigParser parameters = new XmlTopologyConfigParser("topParameters.xml");
		parameters.initParameters();
	
		String topId = parameters.getTopId();
		
		int nbTasks = Integer.parseInt(parameters.getNbTasks());
		int interNbExecutors = Integer.parseInt(parameters.getInterNbExecutors());
		int sinkNbExecutors = Integer.parseInt(parameters.getSinkNbExecutors());
		
		String streamHost = parameters.getSgHost();
		Integer streamPort = Integer.parseInt(parameters.getSgPort());
		
		StreamSimSpout radarSpout = new StreamSimSpout(streamHost, streamPort);
		
		TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("radars", radarSpout).setNumTasks(nbTasks).setCPULoad(10.0).setMemoryLoad(64.0);
        
        builder.setBolt("carMakeProjector", new CarMakeProjector(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("radars")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("carMakeProcessor", new CarMakeProcessor(5350), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("carMakeProjector")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("violationProjector", new ViolationProjector(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("radars")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("speedLimitFilter", new SpeedLimitFilter(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("violationProjector")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("locationProcessor", new LocationProcessor(5351), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("speedLimitFilter")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("registrationProcessor", new RegistrationProcessor(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("speedLimitFilter")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("driverProcessor", new DriverProcessor(5352), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("registrationProcessor")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        /**
         * Configuration of metadata of the topology
         */
        Config config = new Config();
        config.setNumAckers(8);
        config.setNumWorkers(24);
		
		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology(topId, config, builder.createTopology());
	}

}
