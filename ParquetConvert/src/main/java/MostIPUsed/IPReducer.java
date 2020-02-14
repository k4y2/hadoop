package MostIPUsed;

import Utils.MapUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class IPReducer extends Reducer<LongWritable, LongWritable, LongWritable, IntWritable> {
    Map<Long, Integer> mostIP;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mostIP = new LinkedHashMap<>();
    }

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Map<Long, Integer> listGuid = new LinkedHashMap<>();
        for (LongWritable l : values
             ) {
            long guid = l.get();
            listGuid.put(guid, 1);
        }
        mostIP.put(key.get(), listGuid.size());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mostIP = MapUtils.sortByValue(mostIP);
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
