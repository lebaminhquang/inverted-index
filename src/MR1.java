/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author minhquang
 */
public class MR1 {

    public static class MR1Mapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String doc = value.toString();

            //  Split to get username and tweet            
            String[] obj = doc.split(",", 2);
            String userName = obj[0];
            String content = obj[1];
            
            HashSet<String> uniqueSet = new HashSet<>();
            StringTokenizer itr = new StringTokenizer(content);
            while (itr.hasMoreTokens()) {
                uniqueSet.add(String.valueOf(itr.nextToken().hashCode()));
            }
            
            int contentLength = uniqueSet.size();
            
            for(String s : uniqueSet) {
                context.write(new Text(s), new Text(userName + "@" + contentLength));
            }
        }
    }

    public static class MR1Reducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder res = new StringBuilder();
            for (Text val : values) {
                res.append(val.toString()).append(";");
            }
            context.write(key, new Text(res.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        // Map Reduce Job 1: Inverted Index
        long startTime = System.currentTimeMillis();
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MR1");
        job1.setNumReduceTasks(10);
        job1.setJarByClass(MR1.class);
        job1.setMapperClass(MR1Mapper.class);
        job1.setReducerClass(MR1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
        System.out.println("RUN TIME = " + (System.currentTimeMillis() - startTime));
    }
}
