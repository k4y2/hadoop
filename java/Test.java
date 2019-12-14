import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Test {

    public static void main(String[] args) {
        int k = 2;
        Configuration conf = new Configuration();
        conf.set("kmeans.k", k + "");
        try {
            Job job = Job.getInstance(conf);
            job.setJobName("Generate cluster center");

            job.setMapperClass(InitMapper.class);
            job.setReducerClass(InitReducer.class);
            job.setJarByClass(Test.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);


            Path input = new Path(args[0]);
            Path output = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
