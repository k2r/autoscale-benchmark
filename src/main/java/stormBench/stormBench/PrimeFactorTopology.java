/**
 * 
 */
package stormBench.stormBench;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormBench.stormBench.operator.bolt.test.PrimeFactorBolt;
import stormBench.stormBench.operator.spout.test.RandomIntegerSpout;

/**
 * @author Roland
 *
 */
public class PrimeFactorTopology {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		/**
         * Declaration of the linear topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("valGenerator", new RandomIntegerSpout(500000000, 1000000000)).setCPULoad(10).setMemoryLoad(16);
        builder.setBolt("primeFactorizer", new PrimeFactorBolt("primeFactors.txt"), 4).setCPULoad(40).setMemoryLoad(256).shuffleGrouping("valGenerator");
        
        Config config = new Config();
        config.setNumAckers(8);
        config.setNumWorkers(24);
		
		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology("primeFactor", config, builder.createTopology());
	}

}
