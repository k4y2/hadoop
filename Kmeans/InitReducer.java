import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Class reducer khởi tạo trọng tâm các nhóm
 * key là số thứ tự trọng tâm i
 * value là giá trị tọa độ trọng tâm
 */
public class InitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text value : values
        ) {
            context.write(new IntWritable(i), value);
            i++;
        }
    }
}
