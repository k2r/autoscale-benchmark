package stormBench.stormBench;

import java.util.Map;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.shade.com.google.common.collect.Maps;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import stormBench.stormBench.operator.bolt.StarHeatwaveBolt;
import stormBench.stormBench.operator.spout.StarElementSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

public class StarTopology {
	
	protected static final String STAR_TABLE = "star";
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
        JdbcInsertBolt PersistanceBolt = new JdbcInsertBolt(connectionProvider, jdbcMapperBench)
        		.withInsertQuery("insert into star values (?,?,?,?,?,?)")
        		.withQueryTimeoutSecs(5);
        
        /**
         * Declaration of the diamond topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(FieldNames.LYON.toString(), new StarElementSpout(sgHost, sgPort, FieldNames.LYON.toString()));
        
        builder.setSpout(FieldNames.VILLEUR.toString(), new StarElementSpout(sgHost, sgPort, FieldNames.VILLEUR.toString()));
        
        builder.setSpout(FieldNames.VAULX.toString(), new StarElementSpout(sgHost, sgPort, FieldNames.VAULX.toString()));
        
        builder.setBolt("intermediate", new StarHeatwaveBolt())
        .shuffleGrouping(FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.VAULX.toString());
        		
        builder.setBolt("sinkLyon", PersistanceBolt)
        .shuffleGrouping("intermediate", FieldNames.LYON.toString());
        
        builder.setBolt("sinkVilleurbanne", PersistanceBolt)
        .shuffleGrouping("intermediate", FieldNames.VILLEUR.toString());
        
        builder.setBolt("sinkVaulx", PersistanceBolt)
        .shuffleGrouping("intermediate", FieldNames.VILLEUR.toString());
        
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