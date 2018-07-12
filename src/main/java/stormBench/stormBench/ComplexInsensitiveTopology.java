/**
 * 
 */
package stormBench.stormBench;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.xml.sax.SAXException;

import mw16.lsg.storm.OSGCustomGrouping;
import stormBench.stormBench.operator.bolt.benchmark.CityNormalizerInsensitive;
import stormBench.stormBench.operator.bolt.opinion.AgeAnalyzer;
import stormBench.stormBench.operator.bolt.opinion.AgeNormalizer;
import stormBench.stormBench.operator.bolt.opinion.CategoryDispatcher;
import stormBench.stormBench.operator.bolt.opinion.CityAnalyzer;
import stormBench.stormBench.operator.bolt.opinion.OpinionAnalyzer;
import stormBench.stormBench.operator.spout.opinion.SyntheticStreamSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

/**
 * @author Roland
 *
 */
public class ComplexInsensitiveTopology {

	/**
	 * @param args
	 * @throws AuthorizationException 
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, ParserConfigurationException, SAXException, IOException {

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
		int lightNbExecutors = Integer.parseInt(parameters.getInterNbExecutors());
		int heavyNbExecutors = Integer.parseInt(parameters.getSinkNbExecutors());
		Double lightCpuConstraint = Double.parseDouble(parameters.getInterCpuConstraint());
		Double heavyCpuConstraint = Double.parseDouble(parameters.getSinkCpuConstraint());
		Double lightMemConstraint = Double.parseDouble(parameters.getInterMemConstraint());
		Double heavyMemConstraint = Double.parseDouble(parameters.getSinkMemConstraint());
		Integer nbAckers = Integer.parseInt(parameters.getNbAckers());
		Integer nbWorkers = Integer.parseInt(parameters.getNbWorkers());
		
		/**
         * Declaration of the Complex sensitive topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        SyntheticStreamSpout spout = new SyntheticStreamSpout(stream, stateHost, distribution, skew);
       
        builder.setSpout("A", spout, 1).setCPULoad(20).setMemoryLoad(512);
        
        if(grouping.equalsIgnoreCase("shuffle")){
        	builder.setBolt("CategoryDispatcher", new CategoryDispatcher(), lightNbExecutors)
        	.setCPULoad(lightCpuConstraint).setMemoryLoad(lightMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("OpinionSource");

        	builder.setBolt("AgeNormalizer", new AgeNormalizer(), lightNbExecutors)
        	.setCPULoad(lightCpuConstraint).setMemoryLoad(lightMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("CategoryDispatcher", FieldNames.AGE.toString());

        	builder.setBolt("SensitiveBolt", new CityNormalizerInsensitive(), heavyNbExecutors)
        	.setCPULoad(heavyCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("CategoryDispatcher", FieldNames.CITY.toString());

        	builder.setBolt("AgeAnalyzer", new AgeAnalyzer(100), lightNbExecutors)
        	.setCPULoad(lightCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("AgeNormalizer");

        	builder.setBolt("CityAnalyzer", new CityAnalyzer(100), lightNbExecutors)
        	.setCPULoad(lightCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("SensitiveBolt");

        	builder.setBolt("OpinionAnalyzer", new OpinionAnalyzer(), heavyNbExecutors)
        	.setCPULoad(heavyCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
        	.shuffleGrouping("AgeAnalyzer", FieldNames.CATAGE.toString())
        	.shuffleGrouping("CityAnalyzer", FieldNames.NORMCITY.toString());
        }
        
        if(grouping.equalsIgnoreCase("osg")){
            builder.setBolt("B", new CategoryDispatcher(), lightNbExecutors)
            .setCPULoad(lightCpuConstraint).setMemoryLoad(lightMemConstraint).setNumTasks(nbTasks)
            .customGrouping("A", new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("C", new AgeNormalizer(), lightNbExecutors)
            .setCPULoad(lightCpuConstraint).setMemoryLoad(lightMemConstraint).setNumTasks(nbTasks)
            .customGrouping("B",FieldNames.AGE.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("D", new CityNormalizerInsensitive(), heavyNbExecutors)
            .setCPULoad(heavyCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
            .customGrouping("B", FieldNames.CITY.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("E", new AgeAnalyzer(100), lightNbExecutors)
            .setCPULoad(lightCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
            .customGrouping("C", new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("F", new CityAnalyzer(100), lightNbExecutors)
            .setCPULoad(lightCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
            .customGrouping("D", new OSGCustomGrouping(49991L, 0.05, 0.05));
            
            builder.setBolt("G", new OpinionAnalyzer(), heavyNbExecutors)
            .setCPULoad(heavyCpuConstraint).setMemoryLoad(heavyMemConstraint).setNumTasks(nbTasks)
            .customGrouping("E", FieldNames.CATAGE.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05))
            .customGrouping("F", FieldNames.NORMCITY.toString(), new OSGCustomGrouping(49991L, 0.05, 0.05));
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