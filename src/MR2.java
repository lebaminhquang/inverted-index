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
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import org.apache.hadoop.util.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author minhquang
 */
public class MR2 {

    public static class MR2Mapper extends Mapper<Object, Text, Text, Text> {

        private Configuration conf;
        private BufferedReader fis;
        private final Set<String> queryRead = new HashSet<>();

        public static String QUERY = "";
        public static HashSet<String> QUERY_HASH = new HashSet<>();

        private final static Text ONE = new Text(String.valueOf(1));
        public static int K = 10;

        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            URI[] queryURIs = Job.getInstance(conf).getCacheFiles();
            for (URI queryURI : queryURIs) {
                Path queryPath = new Path(queryURI.getPath());
                String queryFileName = queryPath.getName();
                parseQuery(queryFileName);
            }

            for (String i : queryRead) {
                QUERY = i.trim();
                break;
            }

            String[] listOfWords = QUERY.split(" ");
            for (String s : listOfWords) {
                QUERY_HASH.add(String.valueOf(s.hashCode()));
            }
        }

        private void parseQuery(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String query = null;
                while ((query = fis.readLine()) != null) {
                    queryRead.add(query);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("READ LINE =\t" + value.toString());
            String[] line = value.toString().split("\t");
            String word = line[0];

            if (QUERY_HASH.contains(word)) {
                String[] listOfUser = line[1].split(";");
                for (String user : listOfUser) {
                    context.write(new Text(user), ONE);
                }
            }
        }
    }

    public static class MR2Reducer extends Reducer<Text, Text, Text, Text> {

        private IntWritable result = new IntWritable();
        public HashMap<Double, String> hashmap = new HashMap<>();
        public int queryLength = MR2Mapper.QUERY_HASH.size();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                int value = Integer.parseInt(val.toString());
                sum += value;
            }

            String[] userInfo = key.toString().split("@");
            int conjunction = queryLength + Integer.parseInt(userInfo[1]) - sum;

            if (userInfo[0].equals("cocovelvett")) {
                System.out.println("SUM = " + sum);
                System.out.println("Conjunction = " + queryLength + " + " + userInfo[1] + " - " + sum);

            }

            hashmap.put((double) sum / conjunction, userInfo[0]);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Double> keySet = hashmap.keySet();
            List<Double> list = new ArrayList<>(keySet);
            Collections.sort(list);
            int size = list.size() - 1;

            System.out.println("SIZE = " + hashmap.size());

            HashSet<String> uniqueSet = new HashSet<>();
            int k = MR2Mapper.K;

            for (int i = 0; i < k; i++) {
                double SIM = list.get(size - i);
                String user = hashmap.get(SIM);

                if (!uniqueSet.contains(user)) {
                    uniqueSet.add(user);
                    context.write(new Text(user), new Text(String.valueOf(SIM)));
                } else {
                    k++;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Map Reduce Job 1: Inverted Index
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "MR2");
        job1.addCacheFile(new Path(args[2]).toUri());
//        job1.setNumReduceTasks(10);
        job1.setJarByClass(MR2.class);
        job1.setMapperClass(MR2Mapper.class);
        job1.setReducerClass(MR2Reducer.class);
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
