package dataTransformation;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable{
	 public LongArrayWritable(LongWritable[] values) {
	        super(LongWritable.class, values);
	    }

	    @Override
	    public LongWritable[] get() {
	        return (LongWritable[]) super.get();
	    }

	    @Override
	    public void write(DataOutput arg0) throws IOException {
	    	for(LongWritable data : get()){
	            data.write(arg0);
	        }
	    }
	    
	    @Override
	    public String toString() {
	        String output = "";
	        for(LongWritable data : get()){
	    		output = output + " " + data.toString();
	        }
	        return output;
	    }
	    
	    public String printStrings() {
	        String strings = "";
	        for(LongWritable data : get()){
	            strings = strings + " "+ data.toString();
	          }
	         return strings;
	       }
}
