package CalculateTime;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Class mapper dùng để tìm các guid có timeCreate - cookieCreate < 30p
 */
public class TimeMapper extends Mapper<LongWritable, GenericRecord, Text, Text> {
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
        TimeUnit timeUnit = TimeUnit.MINUTES;
        // Lấy giá trị từ file parquet
        long timeCreate = Long.parseLong(value.get("timeCreate").toString());
        long cookieCreate = Long.parseLong(value.get("cookieCreate").toString());
        String guid = value.get("guid").toString();

        // Tính phút và giây của timeCreate - cookieCreate
        long minutes = timeUnit.convert(timeCreate-cookieCreate, TimeUnit.MILLISECONDS);
        timeUnit = TimeUnit.SECONDS;
        long second = timeUnit.convert(timeCreate-cookieCreate, TimeUnit.MILLISECONDS)%60;

        //Kiểm tra điều kiện rồi ghi
        if(minutes<30 && minutes >= 0 && second >= 0) {
            context.write(new Text(guid), new Text(minutes+"p"+second+"s"));
        }
    }
}
