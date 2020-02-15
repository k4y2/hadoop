package MostVisitedUrlEachGuid;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UrlMapper extends Mapper<LongWritable, GenericRecord, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
        // Lấy dữ liệu từ parquet file
        long guid = Long.parseLong(value.get("guid").toString());
        String url = value.get("referer").toString();
        context.write(new LongWritable(guid), new Text(url));
    }
}
