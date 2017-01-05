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

public class AdvertisingTopology {
	
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
        
        builder.setBolt("deserialize", new DeserializeBolt(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.LOGS.toString())
        .setCPULoad(20.0)
        .setMemoryLoad(64.0);
        
        builder.setBolt("eventFilter", new EventFilter(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("deserialize")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("addressProjection", new AdressProjection(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("eventFilter")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("ipProcessor", new IPProcessor(), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("addressProjection")
        .setCPULoad(10.0)
        .setMemoryLoad(64.0);
        
        builder.setBolt("eventProjection", new EventProjection(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("eventFilter")
        .setCPULoad(10.0)
        .setMemoryLoad(32.0);
        
        builder.setBolt("campaignJoin", new CampaignJoin(), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("eventProjection")
        .setCPULoad(75.0)
        .setMemoryLoad(128.0);
        
        builder.setBolt("campaignProcessor", new CampaignProcessor(), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("campaignJoin")
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