package com.briup.Pro_recommend;

import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
Step5
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.MatrixCO

 */
public class MatrixCO extends Configured implements Tool {
    static class MatrixCOMapper extends Mapper<Text, IntWritable,Text,Text>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] ks = key.toString().split(",");
            String k = ks[0];
            String v = ks[1]+":"+value.get();
            context.write(new Text(k),new Text(v));
        }
    }

    static class MatrixCOReducer extends Reducer<Text,Text,Text,Text>{
        private StringBuffer sb = new StringBuffer();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values){
                sb.append(val.toString()).append(",");
            }
            sb.setLength(sb.length()-1);
            context.write(key,new Text(sb.toString()));
            sb.setLength(0);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path StatisticalCo_path = new Path("/user/hdfs/flume/shop_result/statisticalco");
        Path MatrixCO_path = new Path("/user/hdfs/flume/shop_result/matrixco");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(MatrixCO_path)){
            fs.delete(MatrixCO_path,true);
        }

        Job job5 = Job.getInstance(conf);
        job5.setJarByClass(MatrixCO.class);
        job5.setJobName("MatrixCO");

        job5.setMapperClass(MatrixCOMapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setReducerClass(MatrixCOReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job5,StatisticalCo_path);
        SequenceFileOutputFormat.setOutputPath(job5,MatrixCO_path);
        return job5.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new MatrixCO(), args));
    }
}
