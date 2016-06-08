/**
 * 
 */
package stormBench.stormBench.hook;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.task.TopologyContext;
import stormBench.stormBench.utils.FieldNames;

import java.io.Serializable;
import java.sql.*;

/**
 * @author Roland
 *
 */
public class BenchHook extends BaseTaskHook implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8247095798992658077L;
	private static final long initTimestamp = System.currentTimeMillis();
	private static long lastTimestamp;
	private static final long timeBucket = 1000L;
	private static long currentTimestamp;
	
	private TopologyContext context;
	private ArrayList<Integer> operatorTaskIds;
	private HashMap<String, Boolean> operators;
	private HashMap<String, Boolean> sources;
	
	private HashMap<Integer, HashMap<String, Long>> queues;
	private HashMap<Integer, ArrayList<Long>> execTimes;
	private HashMap<Integer, ArrayList<Long>> completeLatencies;
	private HashMap<Integer, Long> throughput;
	private HashMap<Integer, Long> failedTuples;
	
	private final Connection connection;
	private final Statement statement;
	private static Logger logger = Logger.getLogger("BenchHook");
	
	public BenchHook(String dbHost) throws ClassNotFoundException, SQLException{
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/benchmarks";
		String user = "root";
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl,user, null);
		this.statement = this.connection.createStatement();
		
		this.operators = new HashMap<>();
		this.operators.put(FieldNames.SOURCE.toString(), true);
		this.operators.put(FieldNames.INTER.toString(), true);
		this.operators.put(FieldNames.SINK.toString(), true);
		this.operators.put(FieldNames.LYON.toString(), true);
		this.operators.put(FieldNames.VILLEUR.toString(), true);
		this.operators.put(FieldNames.VAULX.toString(), true);
		this.operators.put(FieldNames.SINKLYON.toString(), true);
		this.operators.put(FieldNames.SINKVILL.toString(), true);
		this.operators.put(FieldNames.SINKVLX.toString(), true);
		
		this.sources = new HashMap<>();
		this.sources.put(FieldNames.SOURCE.toString(), true);
		this.sources.put(FieldNames.LYON.toString(), true);
		this.sources.put(FieldNames.VILLEUR.toString(), true);
		this.sources.put(FieldNames.VAULX.toString(), true);
		
		this.operatorTaskIds = new ArrayList<>();
		
		this.init();
	}
	 	
	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#prepare(java.util.Map, backtype.storm.task.TopologyContext)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) {
		this.context = context;
		BenchHook.lastTimestamp = System.currentTimeMillis();
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
		
		BenchHook.currentTimestamp = System.currentTimeMillis();
		if(this.isResettable()){
			this.persistMonitor();
			this.reset();
		}else{
			int taskId = info.taskId;
			if(!this.queues.containsKey(taskId)){
				this.operatorTaskIds.add(taskId);
				createMonitor(taskId);
			}
			HashMap<String, Long> taskQueues = this.queues.get(taskId);
			Long outputs = taskQueues.get("outputs");
			taskQueues.replace("outputs", (outputs + 1));
			this.queues.replace(taskId, taskQueues);
			
			Collection<Integer> outTasks = info.outTasks;
			for(Integer targetTaskId : outTasks){
				if(!this.queues.containsKey(targetTaskId)){
					this.operatorTaskIds.add(targetTaskId);
					createMonitor(targetTaskId);
				}
				HashMap<String, Long> targetTaskQueues = this.queues.get(targetTaskId);
				Long inputs = targetTaskQueues.get("inputs");
				targetTaskQueues.replace("inputs", (inputs + 1));
				this.queues.replace(targetTaskId, targetTaskQueues);
			}
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#spoutAck(backtype.storm.hooks.info.SpoutAckInfo)
	 */
	@Override
	public void spoutAck(SpoutAckInfo info) {
		BenchHook.currentTimestamp = System.currentTimeMillis();
		if(this.isResettable()){
			this.persistMonitor();
			this.reset();
		}else{
			int taskId = info.spoutTaskId;
			if(!this.completeLatencies.containsKey(taskId)){
				this.operatorTaskIds.add(taskId);
				createMonitor(taskId);
			}
			long completeLatencyMs = info.completeLatencyMs;
			ArrayList<Long> taskLatencies = this.completeLatencies.get(taskId);
			taskLatencies.add(completeLatencyMs);
			this.completeLatencies.replace(taskId, taskLatencies);

			long taskThroughput = this.throughput.get(taskId);
			this.throughput.replace(taskId, (taskThroughput + 1));
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#spoutFail(backtype.storm.hooks.info.SpoutFailInfo)
	 */
	@Override
	public void spoutFail(SpoutFailInfo info) {
		BenchHook.currentTimestamp = System.currentTimeMillis();
		if(this.isResettable()){
			this.persistMonitor();
			this.reset();
		}else{
			int taskId = info.spoutTaskId;
			if(!this.failedTuples.containsKey(taskId)){
				this.operatorTaskIds.add(taskId);
				createMonitor(taskId);
			}
			long taskFails = this.failedTuples.get(taskId);
			this.failedTuples.replace(taskId, (taskFails + 1));
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.hooks.ITaskHook#boltExecute(backtype.storm.hooks.info.BoltExecuteInfo)
	 */
	@Override
	public void boltExecute(BoltExecuteInfo info) {
		BenchHook.currentTimestamp = System.currentTimeMillis();
		if(this.isResettable()){
			this.persistMonitor();
			this.reset();
		}else{
			int taskId = info.executingTaskId;
			if(!this.execTimes.containsKey(taskId)){
				this.operatorTaskIds.add(taskId);
				createMonitor(taskId);
			}
			HashMap<String, Long> taskQueues = this.queues.get(taskId);
			Long executed = taskQueues.get("executed");
			taskQueues.replace("executed", (executed + 1));
			this.queues.replace(taskId, taskQueues);

			ArrayList<Long> taskExecTimes = this.execTimes.get(taskId);
			Long execTime = info.executeLatencyMs;
			taskExecTimes.add(execTime);
			this.execTimes.replace(taskId, taskExecTimes);
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
	
	public boolean isOperator(String componentId){
		return this.operators.containsKey(componentId);
	}
	
	public boolean isSource(String componentId){
		return this.sources.containsKey(componentId);
	}
	
	public boolean isResettable(){
		return (BenchHook.currentTimestamp - BenchHook.lastTimestamp) >= BenchHook.timeBucket;
	}
	
	public void persistMonitor(){
		for(Integer taskId : this.operatorTaskIds){
			long timestamp = Math.floorDiv(BenchHook.currentTimestamp - BenchHook.initTimestamp, 1000);
			String componentId = this.context.getComponentId(taskId);

			if(this.operators.containsKey(componentId)){//exclusions of ackers

				/*Operator queues persistence*/
				HashMap<String, Long> tQueues = this.queues.get(taskId);
				long inputs = tQueues.get("inputs");
				long executed = tQueues.get("executed");
				long outputs = tQueues.get("outputs");
				String queryQueues = "INSERT INTO operator_queues VALUES "
						+ "('"+ timestamp +"', '"+ componentId +"', '"+ taskId +"',"
						+ " '"+ inputs +"', '"+ executed +"', '"+ outputs +"')";
				try {
					this.statement.execute(queryQueues);
				} catch (SQLException e) {
					logger.severe("Operator queues persistence has failed because of SQLException " + e);
				}

				/*Average latencies per bolt persistence*/
				ArrayList<Long> tExecTimes = this.execTimes.get(taskId);
				int execSize = tExecTimes.size();
				if(execSize > 0){
					double sum = 0.0;
					for(int i = 0; i < execSize; i++){
						sum += tExecTimes.get(i);
					}
					double avgLatency = sum / execSize;
					String queryLatencies = "INSERT INTO operator_latencies VALUES "
							+ "('"+ timestamp +"', '"+ componentId +"', '"+ taskId +"',"
							+ " '"+ avgLatency +"')";
					try {
						this.statement.execute(queryLatencies);
					} catch (SQLException e) {
						logger.severe("Operator average latencies persistence has failed because of SQLException " + e);
					}

					/*Average CPU load per bolt persistence*/
					double avgCpuLoad = (sum / BenchHook.timeBucket) * 100;
					String queryCpu = "INSERT INTO operator_cpu_loads VALUES "
							+ "('"+ timestamp +"', '"+ componentId +"', '"+ taskId +"',"
							+ " '"+ avgCpuLoad +"')";
					try {
						this.statement.execute(queryCpu);
					} catch (SQLException e) {
						logger.severe("Operator average cpu load persistence has failed because of SQLException " + e);
					}
				}

				/*Topology average end-to-end latency persistence*/
				if(isSource(componentId)){//only Spouts provide information about complete latency, throughput and losses
					ArrayList<Long> cmpltLatencies = this.completeLatencies.get(taskId);
					int cmpltSize = cmpltLatencies.size();
					if(cmpltSize > 0){
						double sum = 0.0;
						for(int i = 0; i < cmpltSize; i++){
							sum += cmpltLatencies.get(i);
						}
						double avgCmpltLatency = sum / cmpltSize;
						String queryCmpltLatencies = "INSERT INTO topology_latencies VALUES "
								+ "('"+ timestamp +"', '"+ componentId +"', '"+ taskId +"',"
								+ " '"+ avgCmpltLatency +"')";
						try {
							this.statement.execute(queryCmpltLatencies);
						} catch (SQLException e) {
							logger.severe("Topology complete latency persistence has failed because of SQLException " + e);
						}
					}

					/*Throughput persistence*/
					Long throughput = this.throughput.get(taskId);
					String queryThroughput = "INSERT INTO topology_throughput VALUES "
							+ "('"+ timestamp +"', '"+ componentId +"', '"+ taskId +"',"
							+ " '"+ throughput +"')";
					try {
						this.statement.execute(queryThroughput);
					} catch (SQLException e) {
						logger.severe("Topology throughput persistence has failed because of SQLException " + e);
					}

					/*Losses persistence*/
					Long losses = this.failedTuples.get(taskId);
					String queryLosses = "INSERT INTO topology_losses VALUES "
							+ "('"+ timestamp +"', '"+ componentId +"', '"+ taskId +"',"
							+ " '"+ losses +"')";
					try {
						this.statement.execute(queryLosses);
					} catch (SQLException e) {
						logger.severe("Topology losses persistence has failed because of SQLException " + e);
					}
				}
			}
		}
	}
	
	public void init(){	
		this.queues = new HashMap<>();
		this.execTimes = new HashMap<>();
		this.completeLatencies = new HashMap<>();
		this.throughput = new HashMap<>();
		this.failedTuples = new HashMap<>();
	}
	
	public void reset(){
		BenchHook.lastTimestamp = BenchHook.currentTimestamp;
		init();
		for(Integer regTaskId : this.operatorTaskIds){
			createMonitor(regTaskId);
		}
	}

	public void createMonitor(Integer taskId){
		HashMap<String, Long> queueCounters = new HashMap<>();
		queueCounters.put("inputs", 0L);
		queueCounters.put("executed", 0L);
		queueCounters.put("outputs", 0L);

		this.queues.put(taskId, queueCounters);
		this.execTimes.put(taskId, new ArrayList<>());
		this.completeLatencies.put(taskId, new ArrayList<>());
		this.throughput.put(taskId, 0L);
		this.failedTuples.put(taskId, 0L);
	}
}