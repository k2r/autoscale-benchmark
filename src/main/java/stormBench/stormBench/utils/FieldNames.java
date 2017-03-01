package stormBench.stormBench.utils;

public enum FieldNames {
	ID("id"),
	CITY("city"),
	ZIP("zipCode"),
	LAT("latitude"),
	LONGIT("longitude"),
	TEMPERATURE("temperature"),
	LYON("Lyon"),
	VILLEUR("Villeurbanne"),
	VAULX("Vaulx"),
	SOURCE("source"),
	INTER("intermediate"),
	SINK("sink"),
	SINKLYON("sinkLyon"),
	SINKVILL("sinkVilleurbanne"),
	SINKVLX("sinkVaulx"),
	USERID("user_id"),
	PGID("page_id"),
	ADID("ad_id"),
	ADTYPE("ad_type"),
	EVTTYPE("event_type"),
	EVTTIME("event_time"),
	IP("ip_address"),
	LOGS("logs"),
	CAMPID("campaign_id"),
	REGISTR("registration"),
	SPEED("speed"),
	MAKE("make"),
	COLOR("color"),
	LOC("location"),
	DRIVER("driver"),
	VALUE("value");
	

	private String name = "";

	FieldNames(String name){
		this.name = name;
	}

	public String toString(){
		return name;
	}

}
