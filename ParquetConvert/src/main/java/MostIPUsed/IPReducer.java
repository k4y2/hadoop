package MostIPUsed;

import Utils.MapUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class IPReducer extends Reducer<LongWritable, LongWritable, LongWritable, IntWritable> {
    // Map lưu trữ ip và số lượng guid
    Map<Long, Integer> mostIP;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mostIP = new LinkedHashMap<>();
    }

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Map<Long, Integer> listGuid = new LinkedHashMap<>();

        // Sử dụng map để lọc cái guid. Các guid trùng nhau sẽ không được đưa vào map
        for (LongWritable value : values
             ) {
            long guid = value.get();
            listGuid.put(guid, 1);
        }

        // Ghi ip và số lượng guid sử dụng ip đó
        mostIP.put(key.get(), listGuid.size());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sắp xếp giảm dần
        mostIP = MapUtils.sortByValue(mostIP);

        // Lấy ra top 1000 giá trị
        int count = 0;
        for (Map.Entry<Long,Integer> entry : mostIP.entrySet()
             ) {
            LongWritable ip = new LongWritable(entry.getKey());
            IntWritable numberOfGuid = new IntWritable(entry.getValue());
            context.write(ip, numberOfGuid);
            count++;
            if(count==1000) break;
        }
    }
}
