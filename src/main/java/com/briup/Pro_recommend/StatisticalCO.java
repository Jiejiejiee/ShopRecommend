package com.briup.Pro_recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
Step4
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.StatisticalCo
 */
public class StatisticalCO extends Configured implements Tool {
    static class StatisticalCoMapper extends Mapper<Text,Text,Text, IntWritable>{
        private  IntWritable val = new IntWritable(1);
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            String v = value.toString();
            if (k.equals(v))return;
            context.write(new Text(k+","+v),val);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path CoOccurrence_path = new Path("/user/hdfs/flume/shop_result/cooccurrence");
        Path StatisticalCo_path = new Path("/user/hdfs/flume/shop_result/statisticalco");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(StatisticalCo_path)){
            fs.delete(StatisticalCo_path,true);
        }

        Job job4 = Job.getInstance(conf);
        job4.setJarByClass(StatisticalCO.class);
        job4.setJobName("StatisticalCO");

        job4.setMapperClass(StatisticalCoMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(IntWritable.class);

        job4.setReducerClass(IntSumReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job4,CoOccurrence_path);
        SequenceFileOutputFormat.setOutputPath(job4,StatisticalCo_path);
        return job4.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new StatisticalCO(), args));
    }
}
