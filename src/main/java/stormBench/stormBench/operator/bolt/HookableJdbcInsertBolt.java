/**
 * 
 */
package stormBench.stormBench.operator.bolt;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import stormBench.stormBench.hook.BenchHook;
import stormBench.stormBench.utils.MetricNames;

/**
 * @author Roland
 *
 */
public class HookableJdbcInsertBolt extends AbstractJdbcBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3844744366187783414L;
	private static Logger logger = Logger.getLogger("HookableJdbcInsertolt");
	
	private JdbcMapper jdbcMapper;
	private String dbHost;
	private String tableName;
	private String insertQuery;
	
	private transient CountMetric cpuAverageLoad;
	private ThreadMXBean threadMXBean;
	
	/**
	 * @param connectionProvider
	 * @param jdbcMapper
	 */
	public HookableJdbcInsertBolt(ConnectionProvider connectionProvider, JdbcMapper jdbcMapper, String dbHost) {
		super(connectionProvider);
		this.jdbcMapper = jdbcMapper;
		this.dbHost = dbHost;
		this.threadMXBean = ManagementFactory.getThreadMXBean();
		if(!threadMXBean.isCurrentThreadCpuTimeSupported()){
			this.threadMXBean.setThreadCpuTimeEnabled(true);
		}
	}
	
	public HookableJdbcInsertBolt withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public HookableJdbcInsertBolt withInsertQuery(String insertQuery) {
        this.insertQuery = insertQuery;
        return this;
    }

    public HookableJdbcInsertBolt withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
        return this;
    }
    
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector){
		super.prepare(arg0, context, collector);
		try {
			context.addTaskHook(new BenchHook(this.dbHost));
		} catch (ClassNotFoundException e) {
			logger.warning("Hook can not be attached to ElementSpout " + HookableJdbcInsertBolt.serialVersionUID + " because the JDBC driver can not be found, error: " + e );
		} catch (SQLException e) {
			logger.warning("Hook can not be attached to ElementSpout " + HookableJdbcInsertBolt.serialVersionUID + " because of invalid JDBC configuration , error: " + e);
		}
		initMetrics(context);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void execute(Tuple tuple) {
        try {
            List<Column> columns = this.jdbcMapper.getColumns(tuple);
            List<List<Column>> columnLists = new ArrayList<List<Column>>();
            columnLists.add(columns);
            if(!StringUtils.isBlank(this.tableName)) {
                this.jdbcClient.insert(this.tableName, columnLists);
            } else {
                this.jdbcClient.executeInsertQuery(this.insertQuery, columnLists);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
        updateMetrics(this.threadMXBean.getCurrentThreadCpuTime());
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	public void initMetrics(TopologyContext context){
		this.cpuAverageLoad = new CountMetric();
		context.registerMetric(MetricNames.CPU.toString(), cpuAverageLoad, 1);
	}
	
	public void updateMetrics(Long threadCpuTimeNs){
		this.cpuAverageLoad.incrBy(threadCpuTimeNs);
	}
	
}
