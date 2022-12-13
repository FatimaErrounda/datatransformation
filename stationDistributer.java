package dataTransformation;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;	

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class stationDistributer {
	static final String DELIMITER = ",";
//	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
//		private Station startstation = new Station();
//		private Station endstation = new Station();
//		final String DELIMITER = ",";
//		final int STARTDATE = 2;
//		final int ENDDATE = 3;
//		final int StartStationName = 4;
//		final int StartStationID = 5;
//		final int EndStationName = 6;
//		final int EndStationID = 7;
//		final int StartStationLat = 8;
//		final int StartStationLong = 9;
//		final int EndStationLat = 10;
//		final int EndStationLong  = 11;
//		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException { 
//			
//			try {
//	            if (key.get() == 0 && value.toString().contains("ride"))
//	                return;
//	            else {
//	            	String[] tokens = value.toString().split(DELIMITER);
//	    			
//	    			startstation.setStationID(new Text(tokens[StartStationID]));
//	    			startstation.setStationName(new Text(tokens[StartStationName]));
//	    			startstation.setStationlat(Double.parseDouble(tokens[StartStationLat]));
//	    			startstation.setStationlong(Double.parseDouble(tokens[StartStationLong]));
//	    			
//	    			endstation.setStationID(new Text(tokens[EndStationID]));
//	    			endstation.setStationName(new Text(tokens[EndStationName]));
//	    			endstation.setStationlat(Double.parseDouble(tokens[EndStationLat]));
//	    			endstation.setStationlong(Double.parseDouble(tokens[EndStationLong]));
//
//	    			DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
//	    			
//	    			try {
//	    				Date startDate = df.parse(tokens[STARTDATE]);
//	    				Timestamp startTimestamp = new Timestamp(startDate.getTime());  
//	    				output.collect(startstation.getStationID(), new LongWritable(startTimestamp.getTime()));
//	    			} catch (ParseException e) {
//	    				e.printStackTrace();
//	    			}
//
//	    			try {
//	    				Date endDate = df.parse(tokens[ENDDATE]);
//	    				Timestamp endTimestamp = new Timestamp(endDate.getTime());  
//	    				output.collect(endstation.getStationID(), new LongWritable(endTimestamp.getTime()));
//	    			} catch (ParseException e) {
//	    				e.printStackTrace();
//	    			}
//	            }
//	        } catch (Exception e) {
//	            e.printStackTrace();
//	        }
//	 }
//	}
//	 
//	public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongArrayWritable> {
//		LongArrayWritable Timestamps;
//		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongArrayWritable> output, Reporter reporter) throws IOException { 
//	 
//			List<Long> list = new ArrayList<Long>();
//		    while (values.hasNext()) {
//		          list.add(values.next().get());
//		    }
//		    Collections.sort(list);  
//		    LongWritable[] TimestampList = new LongWritable[list.size()];
//		    for(int i=0; i< list.size(); i++)
//		    	TimestampList[i] = new LongWritable(list.get(i));
//
//		    output.collect(key, new LongArrayWritable(TimestampList));
//		}
//	}
//	 
//	 public static void main(String[] args) throws Exception { 
//		 JobConf conf = new JobConf(stationDistributer.class); 
//		 conf.setJobName("stationDistributer");
//		 
//		 conf.setOutputKeyClass(Text.class);
//		 conf.setOutputValueClass(LongWritable.class);
//		 
//		 conf.setMapperClass(Map.class);
//		// conf.setCombinerClass(Reduce.class);
//		 conf.setReducerClass(Reduce.class);
//		 Path outputPath = new Path(args[1]);
//		 conf.setInputFormat(TextInputFormat.class);
//		 conf.setOutputFormat(TextOutputFormat.class);
//		 conf.set("mapred.textoutputformat.separator", ",");
//		 outputPath.getFileSystem(conf).delete(outputPath,true);
//		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
//		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//		 
//		 JobClient.runJob(conf);
//	 }
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Station, LongWritable> {
		private Station startstation = new Station();
		private Station endstation = new Station();

		final int STARTDATE = 2;
		final int ENDDATE = 3;
		final int StartStationName = 4;
		final int StartStationID = 5;
		final int EndStationName = 6;
		final int EndStationID = 7;
		final int StartStationLat = 8;
		final int StartStationLong = 9;
		final int EndStationLat = 10;
		final int EndStationLong  = 11;
		final int grid_x = 8;
		final int grid_y = 16;
		static double topleft_x = 40.85035431016681;
		static double topleft_y = -73.9386413618066;
		static double topright_x = 40.833991419252996;
		static double topright_y = -73.89332276229192;
		static double bottomleft_x = 40.73486817825927;
		static double bottomleft_y = -74.01994189221257;
		static double topLength = Math.sqrt(((topleft_x-topright_x)*(topleft_x-topright_x))+((topleft_y-topright_y)*(topleft_y-topright_y)));
		static double sideLength = Math.sqrt(((topleft_x-bottomleft_x)*(topleft_x-bottomleft_x))+((topleft_y-bottomleft_y)*(topleft_y-bottomleft_y)));
		static double slope_top = (topleft_y-topright_y)/(topleft_x-topright_x);
		static double slope_side = (topleft_y-bottomleft_y)/(topleft_x-bottomleft_x);
		static double top_intercept = topleft_y - (slope_top*topleft_x);
		static double side_intercept_left = topleft_y - (slope_side*topleft_x);
		static double bottom_intercept = bottomleft_y - (slope_top*bottomleft_x);
		static double side_intercept_right = topright_y - (slope_side*topright_x);
		public void map(LongWritable key, Text value, OutputCollector<Station, LongWritable> output, Reporter reporter) throws IOException { 
			int station_i=0;
			int station_j=0;
			DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
			StringBuilder str = new StringBuilder();
			try {
	            if (key.get() == 0 && value.toString().contains("ride"))
	                return;
	            else {
	            	
	            	String[] tokens = value.toString().split(DELIMITER);
	            	if(tokens.length >= EndStationLong)
	            	{
	            		if(!tokens[StartStationLat].isEmpty() && !tokens[StartStationLong].isEmpty())
	            		{
			    			double startlat = Double.parseDouble(tokens[StartStationLat]);
			    			double startlong = Double.parseDouble(tokens[StartStationLong]);
			    			if((startlat*slope_top)+top_intercept-startlong > 0 
			    						 && 
			    						 (startlat*slope_top)+bottom_intercept-startlong < 0 
			    						 && 
			    						 (startlat*slope_side)+side_intercept_left-startlong < 0
			    						 && 
			    						 (startlat*slope_side)+side_intercept_right-startlong > 0
			    						 ) 
			    				 {
					    			{
					    				 double test_intercept_top= startlong-(slope_side*startlat);
						   				 double TopProjection_x = (test_intercept_top-top_intercept)/(slope_top-slope_side);
						   				 double TopProjection_y = (TopProjection_x * slope_top) + top_intercept;
						   				 double topTestLength = Math.sqrt(((TopProjection_x-topleft_x)*(TopProjection_x-topleft_x))+((TopProjection_y-topleft_y)*(TopProjection_y-topleft_y)));
						   				 station_i = (int)(grid_x*topTestLength/topLength);
						   				 
						   				 double test_intercept_side= startlong-(slope_top*startlat);
						   				 double SideProjection_x = (test_intercept_side-side_intercept_left)/(slope_side-slope_top);
						   				 double SideProjection_y = SideProjection_x * slope_side + side_intercept_left;
						   				 double sideTestLength = Math.sqrt(((SideProjection_x-topleft_x)*(SideProjection_x-topleft_x))+((SideProjection_y-topleft_y)*(SideProjection_y-topleft_y)));
						   				station_j = (int) (grid_y*sideTestLength/sideLength);
						   				str.append(station_i);
						   				str.append("X");
						   				str.append(station_j);
						   				//String name = Integer.toString(station_i)+"X"+Integer.toString(station_j);
					    				startstation.setStationID(new Text(str.toString()));
					    				startstation.setStationlat(station_i);
					    				startstation.setStationlong(station_j);
						    			try {
						    				Date startDate = df.parse(tokens[STARTDATE]);
						    				Timestamp startTimestamp = new Timestamp(startDate.getTime()); 
						    				//System.out.println(startstation.toString());
						    				output.collect(startstation, new LongWritable(startTimestamp.getTime()));
						    			} catch (ParseException e) {
						    				e.printStackTrace();
						    			}
					    			}
			    				 }
	            		}
	            		if(!tokens[EndStationLat].isEmpty() && !tokens[EndStationLong].isEmpty())
	            		{
			    			double endlat = Double.parseDouble(tokens[EndStationLat]);
			    			double endlong = Double.parseDouble(tokens[EndStationLong]);
			    			if((endlat*slope_top)+top_intercept-endlong > 0 
		   						 && 
		   						 (endlat*slope_top)+bottom_intercept-endlong < 0 
		   						 && 
		   						 (endlat*slope_side)+side_intercept_left-endlong < 0
		   						 && 
		   						 (endlat*slope_side)+side_intercept_right-endlong > 0
		   						 ) 
			    			{
			    				double test_intercept_top= endlong-(slope_side*endlat);
				   				 double TopProjection_x = (test_intercept_top-top_intercept)/(slope_top-slope_side);
				   				 double TopProjection_y = (TopProjection_x * slope_top) + top_intercept;
				   				 double topTestLength = Math.sqrt(((TopProjection_x-topleft_x)*(TopProjection_x-topleft_x))+((TopProjection_y-topleft_y)*(TopProjection_y-topleft_y)));
				   				 station_i = (int)(grid_x*topTestLength/topLength);
				   				 
				   				 double test_intercept_side= endlong-(slope_top*endlat);
				   				 double SideProjection_x = (test_intercept_side-side_intercept_left)/(slope_side-slope_top);
				   				 double SideProjection_y = SideProjection_x * slope_side + side_intercept_left;
				   				 double sideTestLength = Math.sqrt(((SideProjection_x-topleft_x)*(SideProjection_x-topleft_x))+((SideProjection_y-topleft_y)*(SideProjection_y-topleft_y)));
				   				 station_j = (int) (grid_y*sideTestLength/sideLength);
				   				endstation.setStationID(new Text(Integer.toString(station_i)+"X"+Integer.toString(station_j)));
			    				endstation.setStationlat(station_i);
			    				endstation.setStationlong(station_j);
				    			try {
				    				Date endDate = df.parse(tokens[ENDDATE]);
				    				Timestamp endTimestamp = new Timestamp(endDate.getTime());  
				    				//System.out.println(endstation.toString());
				    				output.collect(endstation, new LongWritable(endTimestamp.getTime()));
				    			} catch (ParseException e) {
				    				e.printStackTrace();
				    			}
				    			
			    			}
	            		}
	            	}
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	 }
	}
	 
//	public static class Reduce extends MapReduceBase implements Reducer<Station, LongWritable, Station, Text> {
//		private LongArrayWritable Timestamps;
//		private Duration intervalLength = Duration.ofMinutes(30);
//		public void reduce(Station key, Iterator<LongWritable> values, OutputCollector<Station, Text> output, Reporter reporter) throws IOException { 
			
//	public static class Reduce extends MapReduceBase implements Reducer<Station, LongWritable, Station, LongArrayWritable> {
//		private LongArrayWritable Timestamps;
//		private Duration intervalLength = Duration.ofMinutes(30);
//		public void reduce(Station key, Iterator<LongWritable> values, OutputCollector<Station, LongArrayWritable> output, Reporter reporter) throws IOException { 
	public static class Reduce extends MapReduceBase implements Reducer<Station, LongWritable, Station, TextArrayWritable> {
		private LongArrayWritable Timestamps;
		private Duration intervalLength = Duration.ofMinutes(30);
		public void reduce(Station key, Iterator<LongWritable> values, OutputCollector<Station, TextArrayWritable> output, Reporter reporter) throws IOException { 	 
			DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
			List<Long> list = new ArrayList<Long>();
			int stamplist = 0;
		    while (values.hasNext()) {
		          list.add(values.next().get());
		    }
		    Collections.sort(list);
		    Text[] listTexts = new Text[list.size()];
		    //initialize first timestamp
		    Long initialTimestamp = list.get(0);
		    //initialize first interval
		    Long initialInterval = initialTimestamp + intervalLength.toMillis();
		    //initialize the counter
		    int numberofstamps = 0;
		    //iterate over the list
		    int i=0; 
		    Long timestamp = initialTimestamp;
		    while(i<list.size())
		    {
		    	
		    //if the value is below the interval increase the counter
		    	timestamp = list.get(i);
		    	//System.out.println("Processing "+df.format(timestamp).toString());
		    	if(timestamp<initialInterval)
		    	{
		    		numberofstamps++;
		    	}
		    	else
		    	{
		    		//else reinitialize the timestamp, the interval, the counter, and add the timestamp and counter to the output to collect
		    		Text outputText = new Text(initialTimestamp.toString()+DELIMITER+Integer.toString(numberofstamps));
		    		//System.out.println("the line to add at "+initialTimestamp+" is "+outputText.toString());
		    		listTexts[stamplist] = outputText;
		    		initialTimestamp = timestamp;
		    		initialInterval = initialTimestamp+ intervalLength.toMillis();
		    		numberofstamps=1;
		    		stamplist++;
		    	}
		    	i++;	
		    }
		    if(timestamp<initialInterval && numberofstamps >= 1)
		    {
		    	Text outputText = new Text(initialTimestamp.toString()+DELIMITER+Integer.toString(numberofstamps));
		    	//System.out.println("the line to add at "+initialTimestamp+" is "+outputText.toString());
		    	listTexts[stamplist] = outputText;
		    	stamplist++;
		    }
		    
//		    String t = "";
//		    for(int j=0; j< stamplist; j++)
//		    {
//		    	t+= ","+listTexts[j].toString();
//		    }
//		    output.collect(key, new Text(t));
		    
//		    String t = "";
//		    DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
//		    for(int i=0; i< list.size(); i++)
//		    {
//		    	Date d = new Date(list.get(i));
//		    	
//		    	t += df.format(d).toString() ;
//		    }
//		    output.collect(key, new Text(t));
		    
		    if(stamplist >0)
		    {
		    	output.collect(key, new TextArrayWritable(listTexts));
		    }
//		    LongWritable[] TimestampList = new LongWritable[list.size()];
//		    for(int i=0; i< list.size(); i++)
//		    	TimestampList[i] = new LongWritable(list.get(i));
//
//		    output.collect(key, new LongArrayWritable(TimestampList));
		}
	}
	 
	 public static void main(String[] args) throws Exception { 
		 JobConf conf = new JobConf(stationDistributer.class); 
		 conf.setJobName("stationDistributer");
		 
		 conf.setOutputKeyClass(Station.class);
		 conf.setMapOutputValueClass(LongWritable.class);
		 conf.setOutputValueClass(TextArrayWritable.class);
		 
		 conf.setMapperClass(Map.class);
		 conf.setReducerClass(Reduce.class);
		 Path outputPath = new Path(args[1]);
		 conf.setInputFormat(TextInputFormat.class);
		 //conf.setOutputFormat(TextOutputFormat.class);
		 conf.setOutputFormat(MultipleTextOutputFormatByKey.class);
		 //conf.set("mapred.textoutputformat.separator", ",");
		 outputPath.getFileSystem(conf).delete(outputPath,true);
		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 JobClient.runJob(conf);
	 }
//	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Station, Text> {
//		private Station startstation = new Station();
//		private Station endstation = new Station();
//		final String DELIMITER = ",";
//		final int STARTDATE = 2;
//		final int ENDDATE = 3;
//		final int StartStationName = 4;
//		final int StartStationID = 5;
//		final int EndStationName = 6;
//		final int EndStationID = 7;
//		final int StartStationLat = 8;
//		final int StartStationLong = 9;
//		final int EndStationLat = 10;
//		final int EndStationLong  = 11;
//		public void map(LongWritable key, Text value, OutputCollector<Station, Text> output, Reporter reporter) throws IOException { 
//			
//			try {
//	            if (key.get() == 0 && value.toString().contains("ride"))
//	                return;
//	            else {
//	            	String[] tokens = value.toString().split(DELIMITER);
//	    			
//	    			startstation.setStationID(new Text(tokens[StartStationID]));
//	    			startstation.setStationName(new Text(tokens[StartStationName]));
//	    			startstation.setStationlat(Double.parseDouble(tokens[StartStationLat]));
//	    			startstation.setStationlong(Double.parseDouble(tokens[StartStationLong]));
//	    			
//	    			endstation.setStationID(new Text(tokens[EndStationID]));
//	    			endstation.setStationName(new Text(tokens[EndStationName]));
//	    			endstation.setStationlat(Double.parseDouble(tokens[EndStationLat]));
//	    			endstation.setStationlong(Double.parseDouble(tokens[EndStationLong]));
//
//	    	
//	    			output.collect(startstation, new Text(tokens[STARTDATE]));
//	    		
//	    			output.collect(endstation, new Text(tokens[ENDDATE]));
//	            }		
//	        } catch (Exception e) {
//	            e.printStackTrace();
//	        }
//	 }
//	}
//	 
//	public static class Reduce extends MapReduceBase implements Reducer<Station, Text, Station, Text> {
//		LongArrayWritable Timestamps;
//		public void reduce(Station key, Iterator<Text> values, OutputCollector<Station, Text> output, Reporter reporter) throws IOException { 
//	 
//			String out = "";
//		    while (values.hasNext()) {
//		    	out += values.next().toString();
//		    }
//		    
//		    output.collect(key, new Text(out));
//		}
//	}
//	 
//	 public static void main(String[] args) throws Exception { 
//		 JobConf conf = new JobConf(stationDistributer.class); 
//		 conf.setJobName("stationDistributer");
//		 
//		 conf.setOutputKeyClass(Station.class);
//		 conf.setOutputValueClass(Text.class);
//		 
//		 conf.setMapperClass(Map.class);
//		 //conf.setCombinerClass(Reduce.class);
//		 conf.setReducerClass(Reduce.class);
//		 
//		 conf.setInputFormat(TextInputFormat.class);
//		 conf.setOutputFormat(TextOutputFormat.class);
//		 conf.set("mapred.textoutputformat.separator", ",");
//		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
//		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//		 
//		 JobClient.runJob(conf);
//	 }
}
