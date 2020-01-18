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

    private static Schema schema;

    /**
     * Class Mapper để chuyển dữ liệu từ Text sang Parquet
     * Mapper sẽ xử lý Text -> Avro
     * OutputFormat sẽ xử lý đầu ra từ Avro -> Parquet
     */
    public static class ConvertMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {

        private GenericRecord record;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Đọc dữ liệu và đưa vào GenericRecord
            String line = value.toString();
            String[] values = line.split("\\s+");
            record.put("id", Integer.parseInt(values[0]));
            record.put("Name", values[1]);
            record.put("Dept", values[2]);
            context.write(null,record);
        }

        // Cài đặt Schema
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String schemaStr = conf.get("schema");
            record = new GenericData.Record(new Schema.Parser().parse(schemaStr));
        }
    }

    /**
     * Class Mapper để đọc dữ liệu từ file Parquet
     */
    public static class ReadMapper extends Mapper<LongWritable, GenericRecord, Void, Text> {
        @Override
        protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
            String s = "ID:"+value.get("id")+" Name:"+value.get("Name")+" Dept:"+value.get("Dept");
            context.write(null, new Text(s));
        }
    }

    // Config convertJob
    public static void doConvert(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        File file = new File(args[0]);
        schema = new Schema.Parser().parse(file);
        conf.set("schema", String.valueOf(schema));
        Job job = Job.getInstance(conf, "Convert");

        job.setJarByClass(ConvertParquet.class);
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

    // Config readJob
    public static void doRead(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job2 = Job.getInstance(conf, "Read Parquet");
        job2.setJarByClass(ConvertParquet.class);
        job2.setMapperClass(ReadMapper.class);
        job2.setOutputKeyClass(Void.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(AvroParquetInputFormat.class);
        // Setup AvroParquetInputFormat
        AvroParquetInputFormat.setAvroReadSchema(job2, schema);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]+"/part-m-00000.parquet"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]+"/result"));
        job2.setNumReduceTasks(0);
        job2.waitForCompletion(true);
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            doConvert(conf, args);
            doRead(conf, args);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
