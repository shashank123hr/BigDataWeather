package weathermap;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

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

public class MinTempPerYearStation 
{
    public static class MinTempPerYearStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(11, 18);
            String year = line.substring(71, 75);
            double temperature = 0;
            String temperatureValueRead = line.substring(366, 372).trim();
            
            // Ignoring the first value as its the heading
            if(!temperatureValueRead.equals("") && 
                    !temperatureValueRead.equals("EMNT"))
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
    
    public static class MinTempPerYearStationReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double minTemperature = Integer.MAX_VALUE; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue<minTemperature)
                {
                    minTemperature = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(minTemperature)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(MinTempPerYearStation.class);
        jobConfiguration.setJobName("Finding minimum temperatre per year"
                + "per station");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MinTempPerYearStationMapper.class); 
        jobConfiguration.setReducerClass(MinTempPerYearStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }
}
