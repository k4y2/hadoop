import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class WordCount {
    public static class E_Map extends MapReduceBase implements Mapper<
            Text,
            Text,
            Text,
            IntWritable> {

        public void map(Text text, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] words = line.split("\\s+");
            for (String word : words
                 ) {
                outputCollector.collect(new Text(word), new IntWritable(1));
            };
        }
    }

    public static class E_Reduce extends MapReduceBase implements Reducer<
            Text,
            IntWritable,
            Text,
            IntWritable> {

        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while(iterator.hasNext()) {
                sum += iterator.next().get();
            }
            outputCollector.collect(new Text(text), new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf jobConf = new JobConf();

        jobConf.setJobName("Count Word");

        jobConf.setJarByClass(WordCount.class);

        jobConf.setMapperClass(E_Map.class);
        jobConf.setReducerClass(E_Reduce.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        JobClient.runJob(jobConf);
    }
}
