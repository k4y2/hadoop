import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] coord = line.split(",");
        Configuration conf = context.getConfiguration();
        int k = Integer.parseInt(conf.get("kmeans.k"));
        int clusterID = 0;
        double minDistance = 0;
        for (int i = 0; i < k; i++) {
            String[] clusterCenter = conf.get("kmeans.center"+i).split(",");
            double distance = 0;
            for (int j = 0; j < 2; j++) {
                double x = Double.parseDouble(coord[j]);
                double y = Double.parseDouble(clusterCenter[j]);
                distance += (x-y)*(x-y);
            }
            if(i == 0) {
                minDistance = distance;
                clusterID = 0;
            }
            else {
                if(distance < minDistance) {
                    minDistance = distance;
                    clusterID = i;
                }
            }
        }

        context.write(new IntWritable(clusterID), new Text(line));
    }
}
