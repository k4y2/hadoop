import CalculateTime.TimeMapper;
import ConvertToParquet.ConvertMapper;
import MostIPUsed.IPMapper;
import MostIPUsed.IPReducer;
import MostVisitedUrlEachGuid.UrlMapper;
import MostVisitedUrlEachGuid.UrlReducer;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

import java.io.File;
import java.io.IOException;

public class Main {

    private static Schema schema;

    // Config convertJob
    public static void doConvert(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        File file = new File(args[0]);
        schema = new Schema.Parser().parse(file);
        conf.set("schema", String.valueOf(schema));
        Job job = Job.getInstance(conf, "Convert text to parquet");

        job.setJarByClass(Main.class);
        job.setMapperClass(ConvertMapper.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        // Setup AvroParquet
        AvroParquetOutputFormat.setSchema(job,schema);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

    public static void urlJob(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "Find the most accessed url");
        job.setJarByClass(Main.class);
        job.setMapperClass(UrlMapper.class);
        job.setReducerClass(UrlReducer.class);
        //format
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setAvroReadSchema(job, schema);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        // input - output path
        FileInputFormat.addInputPath(job, new Path(args[2]+"/part-m-00000.parquet"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]+"/MostURL"));
        job.waitForCompletion(true);
    }

    public static void ipJob(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf,"Most IP used");
        job.setJarByClass(Main.class);
        job.setMapperClass(IPMapper.class);
        job.setReducerClass(IPReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setAvroReadSchema(job, schema);

        FileInputFormat.addInputPath(job,new Path(args[2]+"/part-m-00000.parquet"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]+"/MostIP"));
        job.waitForCompletion(true);
    }

    public static void timeJob(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf,"Read date");
        job.setJarByClass(Main.class);
        job.setMapperClass(TimeMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setAvroReadSchema(job,schema);
        FileInputFormat.addInputPath(job,new Path(args[2]+"/part-m-00000.parquet"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]+"/CalculateTime"));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            doConvert(conf, args);
//            urlJob(conf, args);
//            ipJob(conf, args);
            timeJob(conf,args);

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
