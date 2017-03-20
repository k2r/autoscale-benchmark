package stormBench.stormBench.utils;

/**
 * @author Roland
 *
 */
public enum TopologyConfigNodeNames {

	PARAMETERS("parameters"),
	TOPID("topology_id"),
	SGPORT("stream_port"),
	SGHOST("stream_host"),
	NBTASKS("nb_tasks"),
	INTERNBEXECS("intermediate_nb_executors"),
	SINKNBEXECS("sink_nb_executors"),
	INTERCPU("intermediate_cpu_constraint"),
	SINKCPU("sink_cpu_constraint"),
	INTERMEM("intermediate_mem_constraint"),
	SINKMEM("sink_mem_constraint"),
	STATEHOST("state_host"),
	SIZE("window_size"),
	STEP("window_step");
	
	private String name = "";
	
	private TopologyConfigNodeNames(String name) {
		this.name = name;
	}
	
	public String toString(){
		return this.name;
	}
}
