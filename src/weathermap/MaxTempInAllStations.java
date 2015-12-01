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

public class MaxTempInAllStations
{
    public static class MaxTempPerYearStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(11, 18);
            String year = line.substring(71, 78);
            double temperature = 0;
            String temperatureValueRead = line.substring(357, 363).trim();
            
            // Ignoring the first value as its the heading
            if(!temperatureValueRead.equals("") && 
                    !temperatureValueRead.equals("EMXT"))
            {
                temperature = Double.parseDouble(temperatureValueRead);
                if (temperature != 9999.9) 
                {
                    String yearStation = year + "," + station+",";    
                    output.collect(new Text(yearStation), 
                                new DoubleWritable(temperature));
                }
            }
	}
    }
    
    public static class MaxTempPerYearStationReducer extends MapReduceBase implements
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
        JobConf jobConfiguration = new JobConf(MaxTempInAllStations.class);
        jobConfiguration.setJobName("Finding Maximum temperatre per year"
                + "per station");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MaxTempPerYearStationMapper.class); 
        jobConfiguration.setReducerClass(MaxTempPerYearStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }  
}
