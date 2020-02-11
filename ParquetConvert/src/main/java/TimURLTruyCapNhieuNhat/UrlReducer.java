package TimURLTruyCapNhieuNhat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class UrlReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> map = new LinkedHashMap<>();
        Map<String, Integer> sortedMap;
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
        sortedMap = map.entrySet()
                .stream()
                .sorted((Map.Entry.<String, Integer>comparingByValue().reversed()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1,e2) -> e1, LinkedHashMap::new));
        context.write(key,new Text(sortedMap.keySet().toArray()[0].toString()));
    }
}
