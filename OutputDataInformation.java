package dataTransformation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List; 

//read the business and generate the 
public class OutputDataInformation {
	static final String DELIMITER = ",";
	static double smallestlatitude = Double.MAX_VALUE;
	static double largestlatitude = Double.MIN_VALUE;
	static double smallestlongitude  = Double.MAX_VALUE;
	static double largestlongitude =  Double.NEGATIVE_INFINITY;
	//static String mapBusinesses[];
	
	 
	 public static void main(String[] args) throws Exception { 
		 String line = "";  
		 String splitBy = ",";  
		 // get all files in the folder
		 File folder = new File(args[0]);
		 int TotalSamples = 0;
		 List<File> fileList = Arrays.asList(folder.listFiles());
		 Long timestart = Long.MAX_VALUE;
		 Long timeEnd = 0L;
		 System.out.println("filename,min,max,mean,median,std,1stQ,3rdQ");
		 for (Iterator<File> iter = fileList.iterator(); iter.hasNext(); ) {
			 File fileinfo = iter.next();
			 try   
			 {  
				 List<Integer> Statistics=new ArrayList<Integer>(); 
				 List<Long> timestamps=new ArrayList<Long>(); 
			    //parsing a CSV file into BufferedReader class constructor  
			    BufferedReader br = new BufferedReader(new FileReader(args[0]+"/"+fileinfo.getName())); 
			    String headerLine = br.readLine();
			    while ((line = br.readLine()) != null)   //returns a Boolean value  
			    {  
			    	String[] mobility = line.split(splitBy);    // use comma as separator  
			    	Statistics.add(Integer.parseInt(mobility[1])); 
			    	timestamps.add(Long.parseLong(mobility[0]));
			    }  
			    int allStats = Statistics.size();
			    Collections.sort(Statistics);
			    Collections.sort(timestamps);
			    
			    if(timestamps.get(0) > timeEnd)
			    	timeEnd = timestamps.get(0);
			    if(timestamps.get(allStats-1) < timestart)
			    	timestart= timestamps.get(allStats-1);
			    //calculate mean
			    Double mean = 0.0;
			    Double sum = 0.0;
			    Double standardDeviation = 0.0;
			    int median = 0;
			    
			    TotalSamples += allStats;
			    for (Iterator<Integer> st = Statistics.iterator(); st.hasNext(); ) {
			        Integer stat = st.next();
			        sum += stat;
			        
			    }
			    mean = sum/allStats;
			    //calculate median
			    if(allStats%2==1)
				{
					median = Statistics.get((allStats+1)/2-1);
				}
				else
				{
					median=(Statistics.get((allStats/2)-1)+Statistics.get(allStats/2))/2;
				}
			    //calculate standard deviation
			    for (Iterator<Integer> st = Statistics.iterator(); st.hasNext(); ) {
			        Integer stat = st.next();
			        standardDeviation += Math.pow(stat - mean, 2);
			        
			    }
			    standardDeviation = Math.sqrt(standardDeviation/allStats);
//			    1st quarantile
			    int FstQuanrantile = (int) Math.round(allStats * 25 / 100);
			    //3rd quarantile
			    int ThirdQuanrantile = (int) Math.round(allStats * 75 / 100);
			    System.out.println(fileinfo.getName()+","+Statistics.get(0)+","+Statistics.get(allStats-1)+","+median+","+mean+","+standardDeviation+","+Statistics.get(FstQuanrantile)+","+Statistics.get(ThirdQuanrantile));
			 }   
		    catch (IOException e)   
		    {  
		    	e.printStackTrace();  
		    }  

			 
		}
		 System.out.println("TotalSamples "+TotalSamples);
		 System.out.println("timestart" + timestart);
		 System.out.println("timeEnd" + timeEnd);
		
	 }
}
