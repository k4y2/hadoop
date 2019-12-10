import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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

    public static class InitMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(1), value);
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("kmeans.k"));

            int i = 0;
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
                i++;
                if (i == k) break;
            }
        }
    }

    public static class InitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (Text value : values
            ) {
                String s = value.toString();
                s = s + "\t" + "-9";
                context.write(new IntWritable(i), new Text(s));
                i++;
            }
        }
    }
}
