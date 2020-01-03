import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Class Reducer tính toán tọa độ trọng tâm mới
 * dựa vào các tọa độ thuộc trọng tâm đó
 */
public class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String clusterCenter;
        double[] newPoint = new double[2]; // Khởi tạo (x,y) trọng tâm mới
        int count = 0; // Biến đếm số lượng điểm thuộc trọng tâm key
        for (Text t: values // Đọc các điểm thuộc trọng tâm key
             ) {
            String line =  t.toString();
            String[] coord = line.split(","); // Tách ra tọa độ (x,y)
            for (int i = 0; i < 2; i++) {
                newPoint[i]+=Double.parseDouble(coord[i]); // Tính tổng x và y của các điểm
            }
            count++;
        }
        newPoint[0]/=count; // tọa độ x của trọng tâm mới = tổng x các điểm / số lượng điểm
        newPoint[1]/=count; // tọa độ y của trọng tâm mới = tổng y các điểm / số lượng điểm
        clusterCenter = newPoint[0]+","+newPoint[1];
        // Lưu vào file với ID và tọa độ trọng tâm mới
        context.write(key, new Text(clusterCenter));
    }
}
