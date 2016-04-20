/**
 * 
 */
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
import stormBench.stormBench.diamond.bolt.HeatwaveBolt;
import stormBench.stormBench.diamond.spout.ElementSpout;
import stormBench.stormBench.utils.FieldNames;
import stormBench.stormBench.utils.XmlTopologyConfigParser;

/**
 * @author Roland
 *
 */
public class DiamondTopology {

	protected static final String DIAMOND_TABLE = "diamond";
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
		int nbSupervisors = Integer.parseInt(parameters.getNbSupervisors());
		
		Map<String, Object> map = Maps.newHashMap();
    	map.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    	map.put("dataSource.url", "jdbc:mysql://"+ dbHost +"/benchmarks");
    	map.put("dataSource.user","root");

    	/**
    	 * Declaration of source and sink components
    	 */
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(map);
        connectionProvider.prepare();

        JdbcMapper jdbcMapperBench = new SimpleJdbcMapper(DIAMOND_TABLE, connectionProvider);
        JdbcInsertBolt PersistanceBolt = new JdbcInsertBolt(connectionProvider, jdbcMapperBench)
        		.withInsertQuery("insert into diamond values (?,?,?,?,?)")
        		.withQueryTimeoutSecs(5);
        
        ElementSpout spout = new ElementSpout(sgHost, sgPort);
        
        /**
         * Declaration of the diamond topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("source", spout);
        
        builder.setBolt(FieldNames.LYON.toString(), new HeatwaveBolt(FieldNames.LYON.toString(), 28))
        .shuffleGrouping("source", FieldNames.LYON.toString());
        
        builder.setBolt(FieldNames.VILLEUR.toString(), new HeatwaveBolt(FieldNames.VILLEUR.toString(), 30))
        .shuffleGrouping("source", FieldNames.VILLEUR.toString());
        
        builder.setBolt(FieldNames.VAULX.toString(), new HeatwaveBolt(FieldNames.VAULX.toString(), 26))
        .shuffleGrouping("source", FieldNames.VAULX.toString());
        
        builder.setBolt("sink", PersistanceBolt)
        .shuffleGrouping(FieldNames.LYON.toString())
        .shuffleGrouping(FieldNames.VILLEUR.toString())
        .shuffleGrouping(FieldNames.VAULX.toString());
        
        /**
         * Configuration of metadata of the topology
         */
        Config config = new Config();
        config.put(JDBC_CONF, map);
		config.setNumWorkers(nbSupervisors);
		
		/**
		 * Call to the topology submitter for storm
		 */
		StormSubmitter.submitTopology(topId, config, builder.createTopology());
	}
}
