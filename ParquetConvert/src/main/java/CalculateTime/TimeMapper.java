package CalculateTime;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeMapper extends Mapper<LongWritable, GenericRecord, Text, Text> {
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
        long timeCreate = Long.parseLong(value.get("timeCreate").toString());
        long cookieCreate = Long.parseLong(value.get("cookieCreate").toString());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Text keyOut = new Text(dateFormat.format(new Date(timeCreate)));
        Text valueOut = new Text(new Date(cookieCreate).toString());
        context.write(keyOut,valueOut);
    }
}
