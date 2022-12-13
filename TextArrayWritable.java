package dataTransformation;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable  extends ArrayWritable{

	public TextArrayWritable(Text[] values) {
		super(Text.class, values);
	}

	@Override
	public Text[] get()
	{
		return (Text[]) super.get();
	}
	@Override
	public void write(DataOutput arg0) throws IOException 
	{
		for(Text data:get())
			data.write(arg0);
	}
	
	@Override
	public String toString()
	{
		String output = "";
		for(Text data:get())
			output += data.toString();
		return output;
	}
}
