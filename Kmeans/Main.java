import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Hàm main thực thi MapReduce
 */
public class Main {
    /**
     * Method so sánh trọng tâm mới và trọng tâm trước
     * Đồng thời set trọng tâm vào config
     * @param is1 Luồng đọc file trọng tâm thứ i-1
     * @param is2 Luồng đọc file trọng tâm thứ i-2
     * @param k Số lượng trọng tâm
     * @param conf Config job
     * @return Trả về true nếu trọng tâm mới giống trọng tâm trước
     *         Trả về false nếu khác
     */
    public static boolean compare(FSDataInputStream is1, FSDataInputStream is2, int k, Configuration conf) throws IOException {
        boolean check = true;
        for (int j = 0; j < k; j++) {
            // Đọc trọng tâm từ file
            String line1 = is1.readLine();
            String line2 = is2.readLine();
            // Set trọng tâm mới vào config
            String[] tmp = line1.split("\\s+");
            conf.set("kmeans.center"+j, tmp[1]);
            // So sánh 2 trọng tâm ở 2 file
            if(!line1.equalsIgnoreCase(line2)) check = false;
        }
        if(!check) return false;
        return true;
    }

    public static void main(String[] args) {
        if(args.length != 3) {
            System.err.println("Argument: <Input path> <Output path> <Number of center>");
            System.exit(-1);
        }
        int k = Integer.parseInt(args[2]); // Đọc số lượng trọng tâm
        Configuration conf = new Configuration();
        conf.set("kmeans.k", k + ""); // Set k vào config
        try {
            // Khởi tạo job: Tạo các trọng tâm ban đầu
            Job job = Job.getInstance(conf);
            job.setJobName("Generate cluster center");

            job.setMapperClass(InitMapper.class);
            job.setReducerClass(InitReducer.class);
            job.setJarByClass(Main.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            int i = 0;
            while(i < 100) {
                if(i==0) { // Chạy job: Tạo các trọng tâm ban đầu
                    // Set path cho job
                    Path input = new Path(args[0]);
                    Path output = new Path(args[1]+"/cal_"+i);

                    FileInputFormat.addInputPath(job, input);
                    FileOutputFormat.setOutputPath(job, output);
                    job.waitForCompletion(true);
                }
                else { // Chạy job:  tính toán khoảng cách điểm - trọng tâm và tìm trọng tâm mới
                    Configuration conf2 = new Configuration();
                    conf2.set("kmeans.k",k+"");
                    // Đọc file thứ i-1
                    String uri = args[1]+"/cal_"+(i-1)+"/part-r-00000";
                    Path path = new Path(uri);
                    Configuration conf3 = new Configuration();
                    FileSystem fs = FileSystem.get(URI.create(uri),conf3);
                    FSDataInputStream is = fs.open(path);
                    // Đọc file thứ i-2
                    if(i>1) {
                        String uri2 = args[1]+"/cal_"+(i-2)+"/part-r-00000";
                        Path path2 = new Path(uri2);
                        Configuration conf4 = new Configuration();
                        FileSystem fs2 = FileSystem.get(URI.create(uri2),conf4);
                        FSDataInputStream is2 = fs2.open(path2);
                        // So sánh trọng tâm 2 file i-1 và i-2
                        if(compare(is,is2,k,conf2)) {
                            break; // Nếu trọng tâm 2 file giống nhau thì dừng lặp
                        }
                    }
                    else {
                        // Set trọng tâm vào config
                        for (int j = 0; j < k; j++) {
                            String line = is.readLine();
                            String[] tmp = line.split("\\s+");
                            conf2.set("kmeans.center"+j, tmp[1]);
                        }
                    }

                    // Khởi tạo job: tính toán khoảng cách điểm - trọng tâm và tìm trọng tâm mới
                    Job job2 = Job.getInstance(conf2);
                    job2.setJobName("Calculating Cluster "+i);
                    job2.setJarByClass(Main.class);

                    job2.setMapperClass(KmeansMapper.class);
                    job2.setReducerClass(KmeansReducer.class);

                    job2.setOutputKeyClass(IntWritable.class);
                    job2.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job2, new Path(args[0]));
                    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/cal_"+i));
                    job2.waitForCompletion(true);
                }
                i++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }}
