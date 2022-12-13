package dataTransformation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Station implements WritableComparable<Station>{

	private Text stationID;
	//private Text stationName;
	private int stationlat;
	private int stationlong;
	
	public Station() {
		stationID = new Text();
//		stationName = new Text();
		}
	
	public Text getStationID() {
		return stationID;
	}

	public void setStationID(Text stationID) {
		this.stationID = stationID;
	}

//	public Text getStationName() {
//		return stationName;
//	}
//
//	public void setStationName(Text stationName) {
//		this.stationName = stationName;
//	}

	public int getStationlat() {
		return stationlat;
	}

	public void setStationlat(int stationlat) {
		this.stationlat = stationlat;
	}

	public int getStationlong() {
		return stationlong;
	}

	public void setStationlong(int stationlong) {
		this.stationlong = stationlong;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		stationID.readFields(arg0);
//		stationName.readFields(arg0);
		stationlat= arg0.readInt();
		stationlong= arg0.readInt();
//		stationlat= arg0.readDouble();
//		stationlong= arg0.readDouble();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		stationID.write(arg0);
		arg0.writeInt(stationlat);
		arg0.writeInt(stationlong);
//		stationName.write(arg0);
//		arg0.writeDouble(stationlat);
//		arg0.writeDouble(stationlong);
	}
	@Override
	public boolean equals(Object arg0) {
		if( arg0 == null)
			return false;
		if (!(arg0 instanceof Station))
			return false;
		Station other =(Station) arg0;
		return other.getStationID().equals(stationID);
	}
	
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stationID == null) ? 0 : stationID.hashCode());
		return result;
		}

	@Override
	public int compareTo(Station arg0) {
		int cmp = stationID.compareTo(arg0.getStationID());
		if (cmp != 0) {
			return cmp;
			}
		return 0;
	}
	
	@Override
	public String toString() {
	return "Station [name=" + stationID + ", lat=" + stationlat + ", long="
	+ stationlong + "]";
	}
	
}
