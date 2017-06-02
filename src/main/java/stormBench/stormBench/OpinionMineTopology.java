/**
 * 
 */
package stormBench.stormBench;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormBench.stormBench.operator.bolt.opinion.AgeAnalyzer;
import stormBench.stormBench.operator.bolt.opinion.AgeNormalizer;
import stormBench.stormBench.operator.bolt.opinion.CategoryDispatcher;
import stormBench.stormBench.operator.bolt.opinion.CityAnalyzer;
import stormBench.stormBench.operator.bolt.opinion.CityNormalizer;
import stormBench.stormBench.operator.bolt.opinion.OpinionAnalyzer;
import stormBench.stormBench.operator.spout.opinion.StreamSimSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

/**
 * @author Roland
 *
 */
public class OpinionMineTopology {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		/**
		 * Setting of execution parameters
		 */
		XmlTopologyConfigParser parameters = new XmlTopologyConfigParser("topParameters.xml");
		parameters.initParameters();
		
		Integer streamPort = Integer.parseInt(parameters.getSgPort());
		String stateHost = parameters.getStateHost();
		String topId = parameters.getTopId();
		
		int nbTasks = Integer.parseInt(parameters.getNbTasks());
		int interNbExecutors = Integer.parseInt(parameters.getInterNbExecutors());
		int sinkNbExecutors = Integer.parseInt(parameters.getSinkNbExecutors());
		Double interCpuConstraint = Double.parseDouble(parameters.getInterCpuConstraint());
		Double sinkCpuConstraint = Double.parseDouble(parameters.getSinkCpuConstraint());
		Double interMemConstraint = Double.parseDouble(parameters.getInterMemConstraint());
		Double sinkMemConstraint = Double.parseDouble(parameters.getSinkMemConstraint());
		Integer nbAckers = Integer.parseInt(parameters.getNbAckers());
		Integer nbWorkers = Integer.parseInt(parameters.getNbWorkers());
		
		/**
         * Declaration of the Opinion Mining topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        StreamSimSpout spout = new StreamSimSpout(stateHost, streamPort);
        
        builder.setSpout("OpinionSource", spout, 1).setCPULoad(20).setMemoryLoad(512);
        
        builder.setBolt("CategoryDispatcher", new CategoryDispatcher(), interNbExecutors)
        .setCPULoad(interCpuConstraint).setMemoryLoad(interMemConstraint).setNumTasks(nbTasks)
        .shuffleGrouping("OpinionSource");
        
        builder.setBolt("AgeNormalizer", new AgeNormalizer(), interNbExecutors)
        .setCPULoad(interCpuConstraint).setMemoryLoad(interMemConstraint).setNumTasks(nbTasks)
        .shuffleGrouping("CategoryDispatcher", FieldNames.AGE.toString());
        
        builder.setBolt("CityNormalizer", new CityNormalizer(), sinkNbExecutors)
        .setCPULoad(sinkCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        .shuffleGrouping("CategoryDispatcher", FieldNames.CITY.toString());
        
        builder.setBolt("AgeAnalyzer", new AgeAnalyzer(50), interNbExecutors)
        .setCPULoad(interCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        .shuffleGrouping("AgeNormalizer");
        
        builder.setBolt("CityAnalyzer", new CityAnalyzer(50), interNbExecutors)
        .setCPULoad(interCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        .shuffleGrouping("CityNormalizer");
        
        builder.setBolt("OpinionAnalyzer", new OpinionAnalyzer(), sinkNbExecutors)
        .setCPULoad(sinkCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        .shuffleGrouping("AgeAnalyzer", FieldNames.CATAGE.toString())
        .shuffleGrouping("CityAnalyzer", FieldNames.NORMCITY.toString());
        
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
