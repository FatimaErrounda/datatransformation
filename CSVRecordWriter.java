package dataTransformation;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

public class CSVRecordWriter <K, V> implements RecordWriter<K, V>  {

	 // Output Data Stream
    private DataOutputStream out;
    private byte[] newline = "\n".getBytes("UTF-8");
    
    public CSVRecordWriter(DataOutputStream out, String[] headers) throws IOException {
        this.out = out;
        
        // If headers isn't null, print initial header row
        if (headers != null) {        
            for(int i = 0; i < headers.length; i++) {
                writeObject(headers[i]);
                
                if (i != (headers.length - 1)) {
                    out.writeChars(",");
                } 
            }
            out.writeBytes("\n");
        }
      }
    
    private void writeObject(Object o) throws IOException {
        
//        if (o instanceof LongArrayWritable) {
//        	LongArrayWritable lo = (LongArrayWritable) o;
//        	LongWritable[] timestamps = lo.get();
//            for(LongWritable key : timestamps) {
//                out.write(key.toString().getBytes("UTF-8"));
//                out.writeChars(",");
//            }    
//        }
//        else 
        	if (o instanceof TextArrayWritable) {
        	TextArrayWritable lo = (TextArrayWritable) o;
        	Text[] timestamps = lo.get();
            for(Text key : timestamps) {
            	if(key != null)
            	{
            		//key.write(out);
            		out.write(key.toString().getBytes("UTF-8"));
            		out.writeBytes("\n");
            	}
            }    
        }
        	else {  
            out.write(o.toString().getBytes("UTF-8"));
        }
    }
	@Override
	public void close(Reporter arg0) throws IOException {
		out.close();
	}

	@Override
	public void write(K key, V value) throws IOException {
        //writeObject(key);
        //out.writeChars(",");
		if (key == null) {
	        return;
	      }
        writeObject(value);
        //out.writeBytes("\n");
	}

}
