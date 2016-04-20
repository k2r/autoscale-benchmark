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
	DBHOST("database_host"),
	NBSUPERVISORS("nb_supervisors"),
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
