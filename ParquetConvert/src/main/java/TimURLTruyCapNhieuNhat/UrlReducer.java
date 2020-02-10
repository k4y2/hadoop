package TimURLTruyCapNhieuNhat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class UrlReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        SortedMap<String, Integer> map = new TreeMap<>();
        // Read values
        for (Text t : values
             ) {
            String url = t.toString();
            if (!map.containsKey(url)) {
                map.put(url, 1);
            } else {
                map.replace(url, map.get(url) + 1);
            }
        }
        context.write(key,new Text(map.firstKey()));
    }
}
