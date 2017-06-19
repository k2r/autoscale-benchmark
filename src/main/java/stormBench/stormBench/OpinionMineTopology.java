/**
 * 
 */
package stormBench.stormBench;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import mw16.lsg.storm.OSGCustomGrouping;
import stormBench.stormBench.operator.bolt.opinion.AgeAnalyzer;
import stormBench.stormBench.operator.bolt.opinion.AgeNormalizer;
import stormBench.stormBench.operator.bolt.opinion.CategoryDispatcher;
import stormBench.stormBench.operator.bolt.opinion.CityAnalyzer;
import stormBench.stormBench.operator.bolt.opinion.CityNormalizer;
import stormBench.stormBench.operator.bolt.opinion.OpinionAnalyzer;
import stormBench.stormBench.operator.spout.opinion.SyntheticStreamSpout;
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
		
		Integer stream = Integer.parseInt(args[0]);
		String grouping = args[1];
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
        
        SyntheticStreamSpout spout = new SyntheticStreamSpout(stream, stateHost, distribution, skew);
       
        builder.setSpout("OpinionSource", spout, 1).setCPULoad(20).setMemoryLoad(512);
        
        if(grouping.equalsIgnoreCase("shuffle")){
        	builder.setBolt("CategoryDispatcher", new CategoryDispatcher(), interNbExecutors)
        	.setCPULoad(interCpuConstraint).setMemoryLoad(interMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("OpinionSource");

        	builder.setBolt("AgeNormalizer", new AgeNormalizer(), interNbExecutors)
        	.setCPULoad(interCpuConstraint).setMemoryLoad(interMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("CategoryDispatcher", FieldNames.AGE.toString());

        	builder.setBolt("CityNormalizer", new CityNormalizer(), sinkNbExecutors)
        	.setCPULoad(sinkCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("CategoryDispatcher", FieldNames.CITY.toString());

        	builder.setBolt("AgeAnalyzer", new AgeAnalyzer(100), interNbExecutors)
        	.setCPULoad(interCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("AgeNormalizer");

        	builder.setBolt("CityAnalyzer", new CityAnalyzer(100), interNbExecutors)
        	.setCPULoad(interCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("CityNormalizer");

        	builder.setBolt("OpinionAnalyzer", new OpinionAnalyzer(), sinkNbExecutors)
        	.setCPULoad(sinkCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("AgeAnalyzer", FieldNames.CATAGE.toString())
        	.shuffleGrouping("CityAnalyzer", FieldNames.NORMCITY.toString());
        }
        
        if(grouping.equalsIgnoreCase("osg")){
            builder.setBolt("CategoryDispatcher", new CategoryDispatcher(), interNbExecutors)
            .setCPULoad(interCpuConstraint).setMemoryLoad(interMemConstraint).setNumTasks(nbTasks)
            .customGrouping("OpinionSource", new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("AgeNormalizer", new AgeNormalizer(), interNbExecutors)
            .setCPULoad(interCpuConstraint).setMemoryLoad(interMemConstraint).setNumTasks(nbTasks)
            .customGrouping("CategoryDispatcher",FieldNames.AGE.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("CityNormalizer", new CityNormalizer(), sinkNbExecutors)
            .setCPULoad(sinkCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
            .customGrouping("CategoryDispatcher", FieldNames.CITY.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("AgeAnalyzer", new AgeAnalyzer(100), interNbExecutors)
            .setCPULoad(interCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
            .customGrouping("AgeNormalizer", new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("CityAnalyzer", new CityAnalyzer(100), interNbExecutors)
            .setCPULoad(interCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
            .customGrouping("CityNormalizer", new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("OpinionAnalyzer", new OpinionAnalyzer(), sinkNbExecutors)
            .setCPULoad(sinkCpuConstraint).setMemoryLoad(sinkMemConstraint).setNumTasks(nbTasks)
            .customGrouping("AgeAnalyzer", FieldNames.CATAGE.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05))
            .customGrouping("CityAnalyzer", FieldNames.NORMCITY.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05));
        }
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
