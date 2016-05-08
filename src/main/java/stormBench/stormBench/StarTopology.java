package stormBench.stormBench;

import java.util.Map;

import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.shade.com.google.common.collect.Maps;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import stormBench.stormBench.operator.bolt.HookableJdbcInsertBolt;
import stormBench.stormBench.operator.bolt.StarHeatwaveBolt;
import stormBench.stormBench.operator.spout.StarElementSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

public class StarTopology {
	
	protected static final String STAR_TABLE = "results_star";
	protected static final String JDBC_CONF = "jdbc.conf";
	
	public static void main(String[] args) throws Exception {
		
		/**
		 * Setting of execution parameters
		 */
		XmlTopologyConfigParser parameters = new XmlTopologyConfigParser("topParameters.xml");
		parameters.initParameters();
		
		String topId = parameters.getTopId();
		String sgHost = parameters.getSgHost();
		int sgPort = Integer.parseInt(parameters.getSgPort());
		int nbTasks = Integer.parseInt(parameters.getNbTasks());
		int nbExecutors = Integer.parseInt(parameters.getNbExecutors());
		String dbHost = parameters.getDbHost();
		
		Map<String, Object> map = Maps.newHashMap();
    	map.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    	map.put("dataSource.url", "jdbc:mysql://"+ dbHost +"/benchmarks");
    	map.put("dataSource.user","root");

    	/**
    	 * Declaration of source and sink components
    	 */
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(map);
        connectionProvider.prepare();

        JdbcMapper jdbcMapperBench = new SimpleJdbcMapper(STAR_TABLE, connectionProvider);
        HookableJdbcInsertBolt PersistanceBolt = new HookableJdbcInsertBolt(connectionProvider, jdbcMapperBench, dbHost)
        		.withTableName(STAR_TABLE)
        		.withQueryTimeoutSecs(30);
        
        /**
         * Declaration of the star topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(FieldNames.LYON.toString(), new StarElementSpout(sgHost, sgPort, FieldNames.LYON.toString(), dbHost), nbExecutors).setNumTasks(nbTasks);
        
        builder.setSpout(FieldNames.VILLEUR.toString(), new StarElementSpout(sgHost, sgPort, FieldNames.VILLEUR.toString(), dbHost), nbExecutors).setNumTasks(nbTasks);
        
        builder.setSpout(FieldNames.VAULX.toString(), new StarElementSpout(sgHost, sgPort, FieldNames.VAULX.toString(), dbHost), nbExecutors).setNumTasks(nbTasks);
        
        builder.setBolt("intermediate", new StarHeatwaveBolt(dbHost), nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping(FieldNames.LYON.toString(), FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.VILLEUR.toString(), FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.VAULX.toString(), FieldNames.VAULX.toString());
        		
        builder.setBolt("sinkLyon", PersistanceBolt, nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("intermediate", FieldNames.LYON.toString());
        
        builder.setBolt("sinkVilleurbanne", PersistanceBolt, nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("intermediate", FieldNames.VILLEUR.toString());
        
        builder.setBolt("sinkVaulx", PersistanceBolt, nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("intermediate", FieldNames.VAULX.toString());
        
        /**
         * Configuration of metadata of the topology
         */
        Config config = new Config();
        config.setNumAckers(8);
        config.put(JDBC_CONF, map);
		
		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology(topId, config, builder.createTopology());
	}
}