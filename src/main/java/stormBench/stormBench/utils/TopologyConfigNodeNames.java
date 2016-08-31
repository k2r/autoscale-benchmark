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
	NBEXECS("nb_executors"),
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
