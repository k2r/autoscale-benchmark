package stormBench.stormBench;

import java.util.ArrayList;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormBench.stormBench.operator.bolt.SleepBolt;
import stormBench.stormBench.operator.bolt.StarHeatwaveBolt;
import stormBench.stormBench.operator.spout.SyntheticStreamSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

public class StarTopology {
	
	public static void main(String[] args) throws Exception {
		
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
		
    	/**
    	 * Declaration of source and sink components
    	 */
		ArrayList<Integer> codes = new ArrayList<>();
    	codes.add(0);
    	codes.add(1);
    	codes.add(2);
    	
    	//StreamSimSpout spout = new StreamSimSpout(parameters.getSgHost(), Integer.parseInt(parameters.getSgPort()));
    	SyntheticStreamSpout spoutLyon = new SyntheticStreamSpout(stateHost, codes, FieldNames.LYON.toString());
    	
    	SyntheticStreamSpout spoutVilleur = new SyntheticStreamSpout(stateHost, codes, FieldNames.VILLEUR.toString());
    	
    	SyntheticStreamSpout spoutVaulx = new SyntheticStreamSpout(stateHost, codes, FieldNames.VAULX.toString());
    	
        /**
         * Declaration of the star topology
         */
        TopologyBuilder builder = new TopologyBuilder();
                
        builder.setSpout(FieldNames.SOURCE.toString() + FieldNames.LYON.toString(), spoutLyon);
        
        builder.setSpout(FieldNames.SOURCE.toString() + FieldNames.VILLEUR.toString(), spoutVilleur);
        
        builder.setSpout(FieldNames.SOURCE.toString() + FieldNames.VAULX.toString(), spoutVaulx);
        
        builder.setBolt(FieldNames.INTER.toString(), new StarHeatwaveBolt(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.SOURCE.toString() + FieldNames.LYON.toString(), FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.SOURCE.toString() + FieldNames.VILLEUR.toString(), FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.SOURCE.toString() + FieldNames.VAULX.toString(), FieldNames.VAULX.toString());
        		
        builder.setBolt(FieldNames.SINK.toString() + FieldNames.LYON.toString(), new SleepBolt(240), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.LYON.toString());
        
        builder.setBolt(FieldNames.SINK.toString() + FieldNames.VILLEUR.toString(), new SleepBolt(240), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.VILLEUR.toString());
        
        builder.setBolt(FieldNames.SINK.toString() + FieldNames.VAULX.toString(), new SleepBolt(240), sinkNbExecutors).setNumTasks(nbTasks)
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