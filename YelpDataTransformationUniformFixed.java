package dataTransformation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


//read the business and generate the 
public class YelpDataTransformationUniformFixed {
	static final String DELIMITER = ",";
//	static double smallestlatitude = Double.MAX_VALUE;
//	static double largestlatitude = Double.MIN_VALUE;
//	static double smallestlongitude  = Double.MAX_VALUE;
//	static double largestlongitude =  Double.NEGATIVE_INFINITY;
	//static String mapBusinesses[];
	private static java.util.Map<String,Double> latitudeBiz = new HashMap<>();
	private static java.util.Map<String,Double> longitudeBiz = new HashMap<>();
	private static java.util.Map<String,Integer> LocationBiz = new HashMap<>();
	static final int grid_x = 8;
	static final int grid_y = 8;
	
//	static double topleft_x = 49.30352730949539;
//	static double topleft_y = -126.98108591459456;
//	static double topright_x =  48.958471461296746;
//	static double topright_y = -64.09534490995792;
//	static double bottomleft_x = 28.236503380328223;
//	static double bottomleft_y = -124.87171095427134;
	
	static double topleft_x = 28.67021487727133;
	static double topleft_y = -81.480734242986;
	static double topright_x = 28.622583069197862;
	static double topright_y = -81.28326423126379;
	static double bottomleft_x = 28.40644910904251;
	static double bottomleft_y = -81.4703410844743;
	
	static double topLength = Math.sqrt(((topleft_x-topright_x)*(topleft_x-topright_x))+((topleft_y-topright_y)*(topleft_y-topright_y)));
	static double sideLength = Math.sqrt(((topleft_x-bottomleft_x)*(topleft_x-bottomleft_x))+((topleft_y-bottomleft_y)*(topleft_y-bottomleft_y)));
	static double slope_top = (topleft_y-topright_y)/(topleft_x-topright_x);
	static double slope_side = (topleft_y-bottomleft_y)/(topleft_x-bottomleft_x);
	static double top_intercept = topleft_y - (slope_top*topleft_x);
	static double side_intercept_left = topleft_y - (slope_side*topleft_x);
	static double bottom_intercept = bottomleft_y - (slope_top*bottomleft_x);
	static double side_intercept_right = topright_y - (slope_side*topright_x);
	public static void FillMapBusiness(String path)
	{	
		//JSONParser jsonParser = new JSONParser();
		ArrayList<JSONObject> json=new ArrayList<JSONObject>();
	    JSONObject obj;
		String line = null;
		try (FileReader reader = new FileReader(path))
        {
			 BufferedReader bufferedReader = new BufferedReader(reader);

		        while((line = bufferedReader.readLine()) != null) {
		            obj = (JSONObject) new JSONParser().parse(line);
		            json.add(obj);
		            
		        }
		        json.forEach( biz->parseBusiness( (JSONObject) biz ) );
		        // Always close files.
		        System.out.println("total number of buzinesses is '" + json.size());
		        System.out.println("total number of included buzinesses is '" + latitudeBiz.size());
		      
		        double latitude =  28.5739155041893;
		        double longitude = -81.38026704403961;
		        if((latitude*slope_top)+top_intercept-longitude > 0. )
		        	System.out.println("intercept top 1 above 0");
		        if((latitude*slope_top)+bottom_intercept-longitude < 0. )
		        	System.out.println("intercept bottom below 0");
		        if((latitude*slope_side)+side_intercept_left-longitude < 0.)
		        	System.out.println("intercept side 1 below 0");
		        if((latitude*slope_side)+side_intercept_right-longitude > 0.)
		        	System.out.println("intercept side 2 below 0");
		        //System.out.println("smallestlongitude "+smallestlongitude+ " largestlatitude "+largestlatitude+" smallestlatitude "+smallestlatitude +" largestlongitude "+largestlongitude);
//		        for (java.util.Map.Entry<String,Integer> entry : LocationBiz.entrySet())
//		            System.out.println("Key = " + entry.getKey() +
//		                             ", Value = " + entry.getValue());
		        bufferedReader.close();  
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
	}
	public static void parseBusiness(JSONObject biz)
	{
		 String cityObject = (String) biz.get("state");
		 String cityString = (String) biz.get("city");
		 if(cityObject.contains("FL"))
		 {
			 String bizID = (String) biz.get("business_id");
			 double latitude = ((Number)biz.get("latitude")).doubleValue();
			 double longitude = ((Number)biz.get("longitude")).doubleValue();
//			 if(latitude < smallestlatitude)
//				 smallestlatitude = latitude;
//			 if(latitude > largestlatitude)
//				 largestlatitude = latitude;
//			 
//			 if(longitude < smallestlongitude)
//				 smallestlongitude = longitude;
//			 if(longitude > largestlongitude && longitude <0)
//				 largestlongitude = longitude;
			 //if(longitude >= 71)
			//	 System.out.println("found buziness '" + bizID + "' and city is "+cityObject+" latitude is " + latitude+ " and longitude is "+longitude);
	        //Get employee first name 
			 if((latitude*slope_top)+top_intercept-longitude > 0. 
					 && 
					 (latitude*slope_top)+bottom_intercept-longitude < 0. 
					 && 
					 (latitude*slope_side)+side_intercept_left-longitude < 0.
					 && 
					 (latitude*slope_side)+side_intercept_right-longitude > 0.
					 ) 
			 {
				 latitudeBiz.put(bizID, latitude);
	        
			 	 longitudeBiz.put(bizID, longitude);   
			 }
			 
			 if(LocationBiz.containsKey(cityObject))
			 {
				 int numberofinstances = LocationBiz.get(cityObject);
				 numberofinstances++;
				 LocationBiz.put(cityObject, numberofinstances);
			 }
			 else
				 LocationBiz.put(cityObject, 1);
			// System.out.println("found buziness '" + bizID + "' and city is "+cityString+" latitude is " + latitude+ " and longitude is "+longitude);  
			 
		 }
	}
	public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
			
			try {

				JSONParser parser = new JSONParser();
				int station_i=0;
				int station_j=0;
		        Object obj = parser.parse(value.toString());
		        JSONObject jsonObject = (JSONObject) obj;

		        String bizId = (String) jsonObject.get("business_id");
		        StringBuilder str = new StringBuilder();
		        
		        if(latitudeBiz.containsKey(bizId))
		        {
		        	double latitude = latitudeBiz.get(bizId);
		        	double longitude = longitudeBiz.get(bizId);
		        	String checkins = (String) jsonObject.get("date");
		        	
		        	double test_intercept_top= longitude-(slope_side*latitude);
	   				double TopProjection_x = (test_intercept_top-top_intercept)/(slope_top-slope_side);
	   				double TopProjection_y = (TopProjection_x * slope_top) + top_intercept;
	   				double topTestLength = Math.sqrt(((TopProjection_x-topleft_x)*(TopProjection_x-topleft_x))+((TopProjection_y-topleft_y)*(TopProjection_y-topleft_y)));
	   				station_i = (int)(grid_x*topTestLength/topLength);
	   				 
	   				double test_intercept_side= longitude-(slope_top*latitude);
	   				double SideProjection_x = (test_intercept_side-side_intercept_left)/(slope_side-slope_top);
	   				double SideProjection_y = SideProjection_x * slope_side + side_intercept_left;
	   				double sideTestLength = Math.sqrt(((SideProjection_x-topleft_x)*(SideProjection_x-topleft_x))+((SideProjection_y-topleft_y)*(SideProjection_y-topleft_y)));
	   				station_j = (int) (grid_y*sideTestLength/sideLength);
	   				str.append(station_i);
	   				str.append("X");
	   				str.append(station_j);
	   				//DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
	   				//Date startDate = df.parse(checkins);
    				//Timestamp startTimestamp = new Timestamp(startDate.getTime()); 
    		        output.collect(new Text(str.toString()), new Text(checkins));
		        }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	 }
	}
	 
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Station, TextArrayWritable> {
		//private Duration intervalLength = Duration.ofMinutes(30);
		private Duration intervalLength = Duration.ofHours(6);
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Station, TextArrayWritable> output, Reporter reporter) throws IOException { 
			DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
			Long initialTime;
			Long finalTime;
			
			try {
				initialTime = df.parse("17-12-31 23:59:59").getTime();
				finalTime = df.parse("20-12-31 23:59:59").getTime();
			} catch (java.text.ParseException e1) {
				// TODO Auto-generated catch block
				initialTime = 0L;
				finalTime = 0L;
				e1.printStackTrace();
			}
			
			List<Long> list = new ArrayList<Long>();
		    while (values.hasNext()) {
		    	String timestamp = values.next().toString();
		    	try {
					Date date = df.parse(timestamp);
					Timestamp d_timestamp = new Timestamp(date.getTime()); 
					if(date.getTime()>initialTime && date.getTime() < finalTime)
						list.add(d_timestamp.getTime());
				} catch (java.text.ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		    if(list.size() != 0)
		    {
			    Collections.sort(list);
			    int totalElements = (int)((finalTime - initialTime)/intervalLength.toMillis());
			    int[] counts = new int[totalElements];
			    //
			    //initialize first timestamp
			    int i=0;
			    while(i<list.size())
			    {
			    	Long timestamp = list.get(i);
			    	int indexTimestamp = (int)((timestamp - initialTime)/intervalLength.toMillis());
			    	counts[indexTimestamp] += 1;
			    	i++;
			    }
			    Text[] listTexts = new Text[totalElements];
			    Long initialTimestamp = initialTime;
			    for (int j=0;j<totalElements;j++)
			    {	
			    	Text outputText = new Text(initialTimestamp.toString()+DELIMITER+Integer.toString(counts[j]));
			    	listTexts[j] = outputText;
			    	initialTimestamp += intervalLength.toMillis();
			    }
			    Station station = new Station();
		    	station.setStationID(key);
		    	output.collect(station, new TextArrayWritable(listTexts));   	
		    }
		}
	}
	 
	 public static void main(String[] args) throws Exception { 
		 YelpDataTransformationUniformFixed.FillMapBusiness("/home/fatimazahraerrounda/git/dataTransformation/yelp_academic_dataset_business.json");
		 //Configuration configuration = new Configuraiton
		 JobConf conf = new JobConf(YelpDataTransformationUniformFixed.class); 
		 conf.setJobName("YelpDataTransformation");
//		 
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(Text.class);
		 
		 conf.setMapperClass(Map.class);
//		// conf.setCombinerClass(Reduce.class);
		 conf.setReducerClass(Reduce.class);
		 Path outputPath = new Path(args[1]);
		 conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(MultipleTextOutputFormatByKey.class);
		// conf.setOutputFormat(TextOutputFormat.class);
//		 conf.set("mapred.textoutputformat.separator", ",");
		 outputPath.getFileSystem(conf).delete(outputPath,true);
		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 
		 JobClient.runJob(conf);
	 }
}
