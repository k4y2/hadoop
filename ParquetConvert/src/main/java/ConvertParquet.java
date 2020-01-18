import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

import java.io.File;
import java.io.IOException;

public class ConvertParquet {

    public static class ConvertMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\\s+");
            Configuration conf = context.getConfiguration();
            String schemaStr = conf.get("schema");
            GenericRecord record = new GenericData.Record(new Schema.Parser().parse(schemaStr));
            record.put("id", Integer.parseInt(values[0]));
            record.put("Name", values[1]);
            record.put("Dept", values[2]);
            context.write(null,record);
        }

    }

    public static class ReadMapper extends Mapper<LongWritable, Group, Void, Text> {
        @Override
        protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            context.write(null, new Text(s));
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            File file = new File(args[0]);
            Schema schema = new Schema.Parser().parse(file);
            conf.set("schema", String.valueOf(schema));
            Job job = Job.getInstance(conf, "Convert");

            job.setJarByClass(ConvertParquet.class);
            job.setMapperClass(ConvertMapper.class);
            job.setOutputKeyClass(Void.class);
            job.setOutputValueClass(Group.class);
            job.setNumReduceTasks(0);
            job.setOutputFormatClass(AvroParquetOutputFormat.class);
            AvroParquetOutputFormat.setSchema(job,schema);

            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            job.waitForCompletion(true);

            Job job2 = Job.getInstance(conf, "Read Parquet");
            job2.setJarByClass(ConvertParquet.class);
            job2.setMapperClass(ReadMapper.class);
            job2.setOutputKeyClass(Void.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(AvroParquetInputFormat.class);
            AvroParquetInputFormat.setAvroReadSchema(job2, schema);
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]+"/part-m-00000.parquet"));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]+"/result"));
            job2.setNumReduceTasks(0);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
