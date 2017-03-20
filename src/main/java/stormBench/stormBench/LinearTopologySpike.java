package stormBench.stormBench;

import java.util.ArrayList;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormBench.stormBench.operator.bolt.elementary.LinearHeatwaveBolt;
import stormBench.stormBench.operator.bolt.elementary.SleepBolt;
import stormBench.stormBench.operator.spout.elementary.SyntheticSpikeStreamSpout;
//import stormBench.stormBench.operator.spout.StreamSimSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

public class LinearTopologySpike {
	
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
		Double interCpuConstraint = Double.parseDouble(parameters.getInterCpuConstraint());
		Double sinkCpuConstraint = Double.parseDouble(parameters.getSinkCpuConstraint());
		Double interMemConstraint = Double.parseDouble(parameters.getInterMemConstraint());
		Double sinkMemConstraint = Double.parseDouble(parameters.getSinkMemConstraint());
		
    	/**
    	 * Declaration of source and sink components
    	 */
    	ArrayList<Integer> codes = new ArrayList<>();
    	codes.add(0);
    	codes.add(1);
    	codes.add(2);
    	
    	//StreamSimSpout spout = new StreamSimSpout(parameters.getSgHost(), Integer.parseInt(parameters.getSgPort()));
    	SyntheticSpikeStreamSpout spout = new SyntheticSpikeStreamSpout(stateHost, codes);
        /**
         * Declaration of the linear topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(FieldNames.SOURCE.toString(), spout).setCPULoad(10.0).setMemoryLoad(64.0);
        
        builder.setBolt(FieldNames.INTER.toString(), new LinearHeatwaveBolt(), interNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.SOURCE.toString(), FieldNames.VAULX.toString())
        .setCPULoad(interCpuConstraint)
        .setMemoryLoad(interMemConstraint);
        
        builder.setBolt(FieldNames.SINK.toString(), new SleepBolt(80), sinkNbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.INTER.toString(), FieldNames.VAULX.toString())
        .setCPULoad(sinkCpuConstraint)
        .setMemoryLoad(sinkMemConstraint);
        
        /**
         * Configuration of metadata of the topology
         */
        Config config = new Config();
        config.setNumAckers(2);
        config.setNumWorkers(24);
		
		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology(topId, config, builder.createTopology());
	}
}