package MostVisitedUrlEachGuid;

import Utils.MapUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class UrlReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> map = new LinkedHashMap<>();
        Map<String, Integer> sortedMap;
        // Đọc và đếm số lượng url mà guid đó truy cập
        for (Text value : values
             ) {
            String url = value.toString();
            // Sử dụng map để đếm
            if (!map.containsKey(url)) {
                map.put(url, 1);
            } else {
                map.replace(url, map.get(url) + 1);
            }
        }
        // Sắp xếp map giảm dần
        sortedMap = MapUtils.sortByValue(map);
        // Lấy ra url được truy cập nhiều nhất của guid
        context.write(key,new Text(sortedMap.keySet().toArray()[0].toString()));
    }
}
