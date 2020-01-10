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

import java.io.File;
import java.io.IOException;

public class ConvertParquet {

    private static Schema schema;

    public class ConvertMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\\s+");
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", Integer.parseInt(values[0]));
            record.put("Name", values[1]);
            record.put("Dept", values[2]);
            context.write(null, record);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            schema = new Schema.Parser().parse(new File(args[0]));
            Job job = Job.getInstance(conf, "Convert");
            job.setOutputKeyClass(Void.class);
            job.setOutputValueClass(GenericRecord.class);
            job.setNumReduceTasks(0);
            job.setJarByClass(ConvertParquet.class);
            job.setMapperClass(ConvertMapper.class);

            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}