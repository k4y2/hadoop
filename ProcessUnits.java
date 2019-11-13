import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.Iterator;

public class ProcessUnits {
    // Mapper Class
    public static class E_Mapper extends MapReduceBase implements Mapper<
            LongWritable,
            Text,
            Text,
            IntWritable> {
        // Map function
        public void map(LongWritable key, Text value, OutputCollector<Text,
                IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] values = line.split("\\s+");
            String year = values[0];
            int avgPrice = Integer.parseInt(values[values.length-1]);
            output.collect(new Text(year), new IntWritable(avgPrice));
        }
    }

    // Reduce Class
    public static class E_Reduce extends MapReduceBase implements Reducer<
            Text,
            IntWritable,
            Text,
            IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int maxAvg = 30;
            int val = Integer.MIN_VALUE;
            while(values.hasNext()) {
                if((val = values.next().get()) > maxAvg) {
                    output.collect(key,new IntWritable(val));
                }
            }
        }
    }

    // Main func
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(ProcessUnits.class);

        conf.setJobName("Find avg > 30");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(E_Mapper.class);
        conf.setCombinerClass(E_Reduce.class);
        conf.setReducerClass(E_Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
