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
import stormBench.stormBench.operator.bolt.LinearHeatwaveBolt;
import stormBench.stormBench.operator.spout.ElementSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

public class LinearTopology {

	protected static final String LINEAR_TABLE = "lineartopology";
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

        JdbcMapper jdbcMapperBench = new SimpleJdbcMapper(LINEAR_TABLE, connectionProvider);
        HookableJdbcInsertBolt PersistanceBolt = new HookableJdbcInsertBolt(connectionProvider, jdbcMapperBench, dbHost)
        		.withTableName(LINEAR_TABLE)
        		.withQueryTimeoutSecs(30);
        
        ElementSpout spout = new ElementSpout(sgHost, sgPort, dbHost);
        
        /**
         * Declaration of the linear topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("source", spout, nbExecutors).setNumTasks(nbTasks);
        
        builder.setBolt("intermediate", new LinearHeatwaveBolt(dbHost), nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("source", FieldNames.LYON.toString())
        .shuffleGrouping("source", FieldNames.VILLEUR.toString())
        .shuffleGrouping("source", FieldNames.VAULX.toString());
        		
        builder.setBolt("sink", PersistanceBolt, nbExecutors).setNumTasks(nbTasks)
        .shuffleGrouping("intermediate", FieldNames.LYON.toString())
        .shuffleGrouping("intermediate", FieldNames.VILLEUR.toString())
        .shuffleGrouping("intermediate", FieldNames.VAULX.toString());
        
        /**
         * Configuration of metadata of the topology
         */
        Config config = new Config();
        config.put(JDBC_CONF, map);
		
		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology(topId, config, builder.createTopology());
	}
}