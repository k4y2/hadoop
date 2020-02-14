package MostIPUsed;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPMapper extends Mapper<LongWritable, GenericRecord, LongWritable, LongWritable> {
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
        long ip = Long.parseLong(value.get("ip").toString());
        long guid = Long.parseLong(value.get("guid").toString());
        context.write(new LongWritable(ip), new LongWritable(guid));
    }
}
