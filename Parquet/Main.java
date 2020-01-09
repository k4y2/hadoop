import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.File;
import java.io.IOException;


public class Main extends Configured implements Tool {
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
            record.put("id", lineValues[0]);
            record.put("Name", lineValues[1]);
            record.put("Dept", lineValues[2]);
            context.write(null, record);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        schema = new Schema.Parser().parse(new File(args[0]));
        job.setJobName("Convert text to Parquet");
        job.setMapperClass(ConvertMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        AvroParquetOutputFormat.setSchema(job, schema);
        job.setOutputValueClass(GenericRecord.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("<Avro File> <Input> <Output>");
            System.exit(-1);
        }
        try {
            int exitFlag = ToolRunner.run(new Main(), args);
            System.exit(exitFlag);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
