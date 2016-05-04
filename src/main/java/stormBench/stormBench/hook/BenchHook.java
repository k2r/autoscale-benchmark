/**
 * 
 */
package stormBench.stormBench.hook;

import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.task.TopologyContext;
import java.sql.*;

/**
 * @author Roland
 *
 */
public class BenchHook implements ITaskHook {

	TopologyContext context;
	final Connection connection;
	final Statement statement;
	private static Logger logger = Logger.getLogger("BenchHook");
	
	public BenchHook(String dbHost) throws ClassNotFoundException, SQLException{
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/benchmarks";
		String user = "root";
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl,user, null);
		this.statement = this.connection.createStatement();
	}
	 	
	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#prepare(java.util.Map, backtype.storm.task.TopologyContext)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) {
		this.context = context;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#cleanup()
	 */
	@Override
	public void cleanup() {
		logger.info("BenchHook is cleaning up");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#emit(backtype.storm.hooks.info.EmitInfo)
	 */
	@Override
	public void emit(EmitInfo info) {
		int msgId = 0;
		String componentId = this.context.getComponentId(info.taskId);
		String streamId = info.stream;
		for(Integer targetTask : info.outTasks){
			String component = this.context.getComponentId(targetTask);
			String query = "";//TODO write the appropriate query according to new tables
			try {
				this.statement.execute(query);
			} catch (SQLException e) {
				logger.severe("EmitInfo persistence has failed because of SQLException " + e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#spoutAck(backtype.storm.hooks.info.SpoutAckInfo)
	 */
	@Override
	public void spoutAck(SpoutAckInfo info) {
		int msgId = (Integer) info.messageId;
		String componentId = this.context.getComponentId(info.spoutTaskId);
		int taskId = info.spoutTaskId;
		long completeLatencyMs = info.completeLatencyMs;
		String query = "";//TODO write the appropriate query according to new tables
		try {
			this.statement.execute(query);
		} catch (SQLException e) {
			logger.severe("SpoutAckInfo persistence has failed because of SQLException " + e);
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#spoutFail(backtype.storm.hooks.info.SpoutFailInfo)
	 */
	@Override
	public void spoutFail(SpoutFailInfo info) {
		int msgId = (Integer) info.messageId;
		String componentId = this.context.getComponentId(info.spoutTaskId);
		int taskId = info.spoutTaskId;
		long failLatencyMs = info.failLatencyMs;
		String query = "";//TODO write the appropriate query according to new tables
		try {
			this.statement.execute(query);
		} catch (SQLException e) {
			logger.severe("SpoutFailInfo persistence has failed because of SQLException " + e);
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#boltExecute(backtype.storm.hooks.info.BoltExecuteInfo)
	 */
	@Override
	public void boltExecute(BoltExecuteInfo info) {
		int msgId = 0;
		String componentId = this.context.getComponentId(info.executingTaskId);
		int taskId = info.executingTaskId;
		long executeLatencyMs = info.executeLatencyMs;
		String query = ""; //TODO write the appropriate query according to new tables 
		try {
			this.statement.execute(query);
		} catch (SQLException e) {
			logger.severe("BoltExecuteInfo persistence has failed because of SQLException " + e);
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#boltAck(backtype.storm.hooks.info.BoltAckInfo)
	 */
	@Override
	public void boltAck(BoltAckInfo info) {
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#boltFail(backtype.storm.hooks.info.BoltFailInfo)
	 */
	@Override
	public void boltFail(BoltFailInfo info) {
	}
}