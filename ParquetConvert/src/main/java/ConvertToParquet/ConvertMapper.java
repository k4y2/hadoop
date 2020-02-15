package ConvertToParquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class Mapper để chuyển dữ liệu từ Text sang Parquet
 * Mapper sẽ xử lý Text -> Avro
 * OutputFormat sẽ xử lý đầu ra từ Avro -> Parquet
 */
public class ConvertMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {

    private GenericRecord record;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Đọc dữ liệu và đưa vào GenericRecord
        String line = value.toString();
        String[] values = line.split("\\t");
        /* Put dữ liệu date */
        Date timeCreate = null;
        Date cookieCreate = null;
        try {
            timeCreate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(values[0]);
            cookieCreate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(values[1]);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        record.put("timeCreate", timeCreate.getTime());
        record.put("cookieCreate", cookieCreate.getTime());
        record.put("browserCode", Integer.parseInt(values[2]));
        record.put("browserVer", values[3]);
        record.put("osCode", Integer.parseInt(values[4]));
        record.put("osVer", values[5]);
        record.put("ip", Long.parseLong(values[6]));
        record.put("locId", Integer.parseInt(values[7]));
        record.put("domain", values[8]);
        record.put("siteId", Integer.parseInt(values[9]));
        record.put("cId", Integer.parseInt(values[10]));
        record.put("path", values[11]);
        record.put("referer", values[12]);
        record.put("guid", Long.parseLong(values[13]));
        record.put("flashVersion", values[14]);
        record.put("jre", values[15]);
        record.put("sr", values[16]);
        record.put("sc", values[17]);
        record.put("geographic", Integer.parseInt(values[18]));
        context.write(null,record);
    }

    // Cài đặt Schema
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String schemaStr = conf.get("schema");
        record = new GenericData.Record(new Schema.Parser().parse(schemaStr));
    }
}