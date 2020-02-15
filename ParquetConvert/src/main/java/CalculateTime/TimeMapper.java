package CalculateTime;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TimeMapper extends Mapper<LongWritable, GenericRecord, Text, Text> {
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
        TimeUnit timeUnit = TimeUnit.MINUTES;
        long timeCreate = Long.parseLong(value.get("timeCreate").toString());
        long cookieCreate = Long.parseLong(value.get("cookieCreate").toString());
        String guid = value.get("guid").toString();
        long minutes = timeUnit.convert(timeCreate-cookieCreate, TimeUnit.MILLISECONDS);
        timeUnit = TimeUnit.SECONDS;
        long second = timeUnit.convert(timeCreate-cookieCreate, TimeUnit.MILLISECONDS);
        if(minutes<30) {
            context.write(new Text(guid), new Text("00:"+minutes+":"+second));
        }
    }
}
