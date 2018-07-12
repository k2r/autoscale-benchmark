package stormBench.stormBench;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormBench.stormBench.operator.bolt.advertising.AdressProjection;
import stormBench.stormBench.operator.bolt.advertising.CampaignJoin;
import stormBench.stormBench.operator.bolt.advertising.CampaignProcessor;
import stormBench.stormBench.operator.bolt.advertising.DeserializeBolt;
import stormBench.stormBench.operator.bolt.advertising.EventFilter;
import stormBench.stormBench.operator.bolt.advertising.EventProjection;
import stormBench.stormBench.operator.bolt.advertising.IPProcessor;
import stormBench.stormBench.operator.spout.advertising.AdvertisingStreamSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

public class AutoscaleDemoTopology {
	
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
		
    	
    	AdvertisingStreamSpout spout = new AdvertisingStreamSpout(stateHost);
    	
        /**
         * Declaration of the advertising topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(FieldNames.SOURCE.toString(), spout).setCPULoad(10.0).setMemoryLoad(64.0);
        
        builder.setBolt("A", new DeserializeBolt(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.LOGS.toString())
        .setCPULoad(20.0)
        .setMemoryLoad(64.0);
        
        builder.setBolt("B", new EventFilter(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("A")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("C", new AdressProjection(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("B")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("D", new IPProcessor(), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("C")
        .setCPULoad(10.0)
        .setMemoryLoad(64.0);
        
        builder.setBolt("E", new EventProjection(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("B")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("F", new CampaignJoin(), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("E")
        .setCPULoad(75.0)
        .setMemoryLoad(128.0);
        
        builder.setBolt("G", new CampaignProcessor(), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("F")
        .setCPULoad(20.0)
        .setMemoryLoad(64.0);
        
        
        
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