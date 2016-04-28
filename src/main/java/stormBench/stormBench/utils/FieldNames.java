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
	VAULX("Vaulx");
	

	private String name = "";

	FieldNames(String name){
		this.name = name;
	}

	public String toString(){
		return name;
	}

}
