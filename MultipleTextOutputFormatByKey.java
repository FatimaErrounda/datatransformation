package dataTransformation;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

public class MultipleTextOutputFormatByKey extends MultipleOutputFormat<Station, TextArrayWritable>{

	@Override
	protected String generateFileNameForKeyValue(Station key, TextArrayWritable value, String name) {
		return key.getStationID().toString();
	}
	
	@Override
	protected RecordWriter<Station, TextArrayWritable> getBaseRecordWriter(FileSystem filesystem, JobConf conf, String name,
			Progressable pr) throws IOException {
		Path file= new Path(name + ".csv");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, pr);        
        return new CSVRecordWriter<Station, TextArrayWritable>(fileOut, conf.getStrings("csv.header.fields", "timestamp,stats"));
	}

}
