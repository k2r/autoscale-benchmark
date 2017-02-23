/**
 * 
 */
package stormBench.stormBench;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import mw16.lsg.storm.OSGCustomGrouping;
import stormBench.stormBench.operator.bolt.osg.KeySensitiveBolt;
import stormBench.stormBench.operator.spout.osg.ZipfIntegerSpout;

/**
 * @author Roland
 *
 */
public class OSGTestTopology {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		String command = args[0];
		/**
		 * Declaration of the OSG test topology
		 */
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("zipfIntegerGenerator", new ZipfIntegerSpout(4096, 1.5), 1).setCPULoad(5).setMemoryLoad(256);
		
		if(command.equalsIgnoreCase("osg")){
			builder.setBolt("keySensitiveBolt", new KeySensitiveBolt(64, 1.5), 1)
			.setNumTasks(16).setCPULoad(20).setMemoryLoad(128).customGrouping("zipfIntegerGenerator", new OSGCustomGrouping(10000019L, 0.05, 0.05));
		}
		
		if(command.equalsIgnoreCase("shuffle")){
			builder.setBolt("keySensitiveBolt", new KeySensitiveBolt(64, 1.5), 1)
			.setNumTasks(16).setCPULoad(20).setMemoryLoad(128).shuffleGrouping("zipfIntegerGenerator");
		}
		
		Config config = new Config();
		config.setNumAckers(8);
		config.setNumWorkers(24);

		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology("osgTest", config, builder.createTopology());
	}
}
