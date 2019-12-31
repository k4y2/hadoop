import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String clusterCenter;
        double[] newPoint = new double[2];
        for (int i = 0; i < 2; i++) {
            int count = 0;
            double sum = 0;
            for (Text t: values
            ) {
                String line =  t.toString();
                String[] coord = line.split(",");
                sum += Integer.parseInt(coord[i]);
                count++;
            }
            newPoint[i] = sum/count;
        }
        clusterCenter = newPoint[0]+","+newPoint[1];
        context.write(key, new Text(clusterCenter));
    }
}
