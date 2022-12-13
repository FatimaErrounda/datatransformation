package dataTransformation;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

public class MultipleLongOutputFormatByKey extends MultipleOutputFormat<Station, LongArrayWritable> {

	@Override
	protected String generateFileNameForKeyValue(Station key, LongArrayWritable value, String name) {
		return key.getStationID().toString();
	}
	
	@Override
	protected RecordWriter<Station, LongArrayWritable> getBaseRecordWriter(FileSystem filesystem, JobConf conf, String name,
			Progressable pr) throws IOException {
		Path file= new Path(name + ".csv");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, pr);        
        return new CSVRecordWriter<Station, LongArrayWritable>(fileOut, conf.getStrings("csv.header.fields", "timestamp,stats"));
	}

	}
