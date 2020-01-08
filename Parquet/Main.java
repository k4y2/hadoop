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
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;


public class Main {
    private static Schema schema;

    public class ConvertMapper extends Mapper<LongWritable,
            Text,
            Void,
            GenericRecord> {
        private GenericRecord record = new GenericData.Record(schema);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineValues = line.split("\\s+");
            record.put("id",lineValues[0]);
            record.put("Name",lineValues[1]);
            record.put("Dept",lineValues[2]);
            context.write(null, record);
        }
    }
    public static void main(String[] args) {
        if(args.length != 3) {
            System.err.println("<Avro File> <Input> <Output>");
            System.exit(-1);
        }
        schema = new Schema.Parser().parse(args[0]);
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJobName("Convert text to Parquet");
            job.setJarByClass(Main.class);
            job.setMapperClass(ConvertMapper.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Void.class);
            job.setOutputValueClass(GenericRecord.class);
            job.setOutputFormatClass(AvroParquetOutputFormat.class);

            FileInputFormat.addInputPath(job,new Path(args[1]));
            FileOutputFormat.setOutputPath(job,new Path(args[2]));
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
