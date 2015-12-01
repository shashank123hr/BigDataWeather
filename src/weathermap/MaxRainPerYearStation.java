package weathermap;

/**
 *
 * @author
 * Nikhil Swami
 * Shashank HR
 * Vignesh Dhamodaran
 */

/*
* #ToDo
* Change the Name of Mapper and Reducer
* Comments
*/

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MaxRainPerYearStation 
{
    public static class MaxRainPerYearStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(11, 18);
            String year = line.substring(71, 75);
            double precipitation = 0;
            String precipitationValueRead = line.substring(312, 318).trim();
            
            // Ignoring the first value as its the heading
            if(!precipitationValueRead.equals("") && 
                    !precipitationValueRead.equals("EMXP"))
            {
                precipitation = Double.parseDouble(precipitationValueRead);
                if (precipitation != 99.99) 
                {
                    String yearStation = year + "," + station+",";    
                    output.collect(new Text(yearStation), 
                                new DoubleWritable(precipitation));
                }
            }
	}
    }
    
    public static class MaxRainPerYearStationReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double maxRain = 0; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue>maxRain)
                {
                    maxRain = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(maxRain)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(MaxRainPerYearStation.class);
        jobConfiguration.setJobName("Finding Maximum rain per year"
                + "per station");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MaxRainPerYearStationMapper.class); 
        jobConfiguration.setReducerClass(MaxRainPerYearStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }  
}
