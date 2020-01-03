import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class Mapper tính toán khoảng cách giữa các điểm và trọng tâm
 * Tập hợp các điểm gần trọng tâm nào đó nhất
 */
public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); // Đọc giá trị tọa độ
        String[] coord = line.split(","); // Tách lấy tọa độ x,y
        Configuration conf = context.getConfiguration();
        int k = Integer.parseInt(conf.get("kmeans.k")); // Lấy số trọng tâm k
        int clusterID = 0;
        double minDistance = 0;
        // Duyệt từng trọng tâm 1
        for (int i = 0; i < k; i++) {
            // Lấy ra tọa độ trọng tâm i được lưu trong config
            String[] clusterCenter = conf.get("kmeans.center"+i).split(",");
            double distance = 0;
            // Tính khoảng cách giữa điểm đang đọc và trọng tâm thứ i
            for (int j = 0; j < 2; j++) {
                double x = Double.parseDouble(coord[j]);
                double y = Double.parseDouble(clusterCenter[j]);
                distance += (x-y)*(x-y);
            }
            if(i == 0) {
                // Khởi tạo khoảng cách nhỏ nhất
                minDistance = distance;
                clusterID = 0;
            }
            else {
                // Nếu khoảng cách đến trọng tâm sau nhỏ hơn minDistance
                if(distance < minDistance) {
                    minDistance = distance;
                    clusterID = i; // Nhận trọng tâm mới
                }
            }
        }

        //Output: (key,value) - (ID Trọng tâm, tọa độ điểm đang đọc)
        context.write(new IntWritable(clusterID), new Text(line));
    }
}
