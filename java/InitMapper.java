import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class mapper khởi tạo trọng tâm các nhóm
 * k: Số trọng tâm
 */
public class InitMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(1), value);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int k = Integer.parseInt(conf.get("kmeans.k")); // Lấy ra số trọng tâm k

        int i = 0;
        // Đọc giá trị
        while (context.nextKeyValue()) {
            // Map các tọa độ đầu trong danh sách làm trọng tâm
            map(context.getCurrentKey(), context.getCurrentValue(), context);
            i++;
            if (i == k) break;
        }
    }
}
