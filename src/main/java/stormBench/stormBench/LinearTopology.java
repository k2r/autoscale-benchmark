package stormBench.stormBench;

import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import stormBench.stormBench.operator.bolt.SleepBolt;
import stormBench.stormBench.operator.bolt.LinearHeatwaveBolt;
//import stormBench.stormBench.operator.spout.StreamSimSpout;
import stormBench.stormBench.operator.spout.SyntheticStreamSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

public class LinearTopology {
	
	public static void main(String[] args) throws Exception {
		
		/**
		 * Setting of execution parameters
		 */
		XmlTopologyConfigParser parameters = new XmlTopologyConfigParser("topParameters.xml");
		parameters.initParameters();
		
		String stateHost = parameters.getStateHost();
		String topId = parameters.getTopId();
		
		int nbTasks = Integer.parseInt(parameters.getNbTasks());
		int nbExecutors = Integer.parseInt(parameters.getNbExecutors());
		
    	/**
    	 * Declaration of source and sink components
    	 */
    	ArrayList<Integer> codes = new ArrayList<>();
    	codes.add(0);
    	codes.add(1);
    	codes.add(2);
    	
    	//StreamSimSpout spout = new StreamSimSpout(parameters.getSgHost(), Integer.parseInt(parameters.getSgPort()));
    	SyntheticStreamSpout spout = new SyntheticStreamSpout(stateHost, codes);
        /**
         * Declaration of the linear topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(FieldNames.SOURCE.toString(), spout);
        
        builder.setBolt(FieldNames.INTER.toString(), new LinearHeatwaveBolt(), nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.VAULX.toString());
        
        builder.setBolt(FieldNames.SINK.toString(), new SleepBolt(80), nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.VAULX.toString());
        
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