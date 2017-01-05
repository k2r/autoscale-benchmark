/**
 * 
 */
package stormBench.stormBench.operator.meta;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

/**
 * @author Roland
 *
 */
public class SampleHook extends BaseTaskHook implements Serializable{

	private TopologyContext context;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8331951328890007552L;
	private static Logger logger = Logger.getLogger("SampleHook");
	
	public SampleHook(){
		logger.info("Initialization of the sample hook");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) {
		this.context = context;
		logger.info("This method is automatically when a Hook is enabled (activation of the topology)");
		logger.info("To attach this Hook to any component (spout/bolt), simply add the following line in the prepare() method of the component");
		logger.info("context.addTaskHook(new SampleHook())");
		logger.info("it works without any other configuration #Magic");
		int taskId = 0;
		this.context.getComponentId(taskId);// Returns the component id linked to the task with the id taskId 
	}
	
	@Override
	public void cleanup() {
		logger.info("This method is automatically called each time the Hook is turned off (deactivation of the the topology of reaffectation of executors)");
	}
	
	@SuppressWarnings("unused")
	@Override
	public void emit(EmitInfo info) {
		logger.info("This method is called automatically each time a component emits a new tuple (acked or not)");
		Collection<Integer> outTasks = info.outTasks;// Returns identifiers of destination tasks
		String stream = info.stream; // Returns the name of the stream where the tuple was emitted on
		int taskId = info.taskId; //Returns the taskId of the emitting task
		List<Object> values = info.values; //Returns the emitted tuple under the form of a list, the order matchs with the declared fields
	}
	
	@SuppressWarnings("unused")
	@Override
	public void spoutAck(SpoutAckInfo info) {
		logger.info("This method is called automatically each time a spout acks a tuple (i.e. a tuple reaches an exit bolt without failure)");
		int taskId = info.spoutTaskId; // Returns the taskId of the acking spout
		Long latency = info.completeLatencyMs; // Returns the complete time spent by the tuple within the topology
		Object messageId = info.messageId; // Returns the id of the tuple (the one defined at the emission from the spout)
	}
	
	@SuppressWarnings("unused")
	@Override
	public void spoutFail(SpoutFailInfo info) {
		logger.info("This method is called automatically each time a tuple failed to be processed within a given duration (per default 30sec)");
		int taskId = info.spoutTaskId; // Returns the taskId of the acking spout
		Long latency = info.failLatencyMs; // Returns the complete time spent by the tuple within the topology
		Object messageId = info.messageId; // Returns the id of the tuple (the one defined at the emission from the spout)
	}
	
	@SuppressWarnings("unused")
	@Override
	public void boltExecute(BoltExecuteInfo info) {
		logger.info("This method is called automatically each time a tuple is processed (it does not include time spent in pending queues)");
		int taskId = info.executingTaskId; // Returns the taskId of the executing bolt
		Long latency = info.executeLatencyMs; // Returns the execution time of the tuple
		Tuple tuple = info.tuple; // Returns the executed tuple
	}
	
	@SuppressWarnings("unused")
	@Override
	public void boltAck(BoltAckInfo info) {
		logger.info("This method is called automatically each time a bolt acks a tuple (i.e. an explicit call to the method collector.ack(Tuple)");
		int taskId = info.ackingTaskId; // Returns the taskId of the acking bolt
		Long latency = info.processLatencyMs; // Returns the processing time of the tuple (execution + emission of the associated result)
		Tuple tuple = info.tuple; // Returns the acked tuple
	}
	
	@SuppressWarnings("unused")
	@Override
	public void boltFail(BoltFailInfo info) {
		logger.info("This method is called automatically each time a bolt failed a tuple (i.e. the tuple exceeds its Time-To-Live within the current operator)");
		int taskId = info.failingTaskId; // Returns the taskId of the acking bolt
		Long latency = info.failLatencyMs; // Returns the processing time of the tuple (execution + emission of the associated result) at failure
		Tuple tuple = info.tuple; // Returns the failed tuple
	}
}
