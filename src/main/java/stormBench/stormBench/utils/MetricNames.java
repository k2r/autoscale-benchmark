package stormBench.stormBench.utils;

public enum MetricNames {
	
	OUTPUT("outputQueueSize"),
	CPU("cpuAverageLoad");
	
	private String name = "";

	MetricNames(String name){
		this.name = name;
	}

	public String toString(){
		return name;
	}
}
