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

public class Test {

    public static boolean compare(FSDataInputStream is1, FSDataInputStream is2, int k, Configuration conf) throws IOException {
        boolean check = true;
        for (int j = 0; j < k; j++) {
            String line1 = is1.readLine();
            String line2 = is2.readLine();
            String[] tmp = line1.split("\\s+");
            conf.set("kmeans.center"+j, tmp[1]);
            if(!line1.equalsIgnoreCase(line2)) check = false;
        }
        if(!check) return false;
        return true;
    }

    public static void main(String[] args) {
        int k = 2;
        boolean stop = false;
        Configuration conf = new Configuration();
        conf.set("kmeans.k", k + "");
        InitMapper mapper = new InitMapper();
        InitReducer reducer = new InitReducer();
        try {
            Job job = Job.getInstance(conf);
            job.setJobName("Generate cluster center");

            job.setMapperClass(mapper.getClass());
            job.setReducerClass(reducer.getClass());
            job.setJarByClass(Test.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            int i = 0;
            while(i < 10 && !stop) {
                if(i==0) {
                    Path input = new Path(args[0]);
                    Path output = new Path(args[1]+"/cal_"+i);
                    FileInputFormat.addInputPath(job, input);
                    FileOutputFormat.setOutputPath(job, output);
                    job.waitForCompletion(true);
                }
                else {
                    Configuration conf2 = new Configuration();
                    conf2.set("kmeans.k",k+"");
                    String uri = args[1]+"/cal_"+(i-1)+"/part-r-00000";
                    Path path = new Path(uri);
                    Configuration conf3 = new Configuration();
                    FileSystem fs = FileSystem.get(URI.create(uri),conf3);
                    FSDataInputStream is = fs.open(path);
                    if(i>1) {
                        String uri2 = args[1]+"/cal_"+(i-2)+"/part-r-00000";
                        Path path2 = new Path(uri2);
                        Configuration conf4 = new Configuration();
                        FileSystem fs2 = FileSystem.get(URI.create(uri2),conf4);
                        FSDataInputStream is2 = fs2.open(path2);
                        if(compare(is,is2,k,conf2)) {
                            stop=true;
                        }
                    }
                    else {
                        for (int j = 0; j < k; j++) {
                            String line = is.readLine();
                            String[] tmp = line.split("\\s+");
                            conf2.set("kmeans.center"+j, tmp[1]);
                        }
                    }
                    Job job2 = Job.getInstance(conf2);
                    job2.setJobName("Calculating Cluster "+i);
                    job2.setJarByClass(Test.class);

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
    }
}
