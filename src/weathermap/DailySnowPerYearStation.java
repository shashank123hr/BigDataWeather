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

public class DailySnowPerYearStation 
{
    public static class MaxWindPerYearStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(11, 18);
            String year = line.substring(71, 75);
            double windSpeed = 0;
            String windSpeedValueRead = line.substring(330, 336).trim();
            
            // Ignoring the first value as its the heading
            if(!windSpeedValueRead.equals("") && 
                    !windSpeedValueRead.equals("DSNW"))
            {
                windSpeed = Double.parseDouble(windSpeedValueRead);
                if (windSpeed != 999.9) 
                {
                    String yearStation = year + "," + station+",";    
                    output.collect(new Text(yearStation), 
                                new DoubleWritable(windSpeed));
                }
            }
	}
    }
    
    public static class MaxWindPerYearStationReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double maxTemperature = 0; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue>maxTemperature)
                {
                    maxTemperature = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(maxTemperature)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(DailySnowPerYearStation.class);
        jobConfiguration.setJobName("Finding Maximum wind per year"
                + "per station");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MaxWindPerYearStationMapper.class); 
        jobConfiguration.setReducerClass(MaxWindPerYearStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }  
}
