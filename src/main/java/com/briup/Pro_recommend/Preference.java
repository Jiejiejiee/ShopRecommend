package com.briup.Pro_recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
Step1
计算每个用户对某个商品的偏好值之和
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.Preference -D input=/user/hdfs/flume/shop
 */
public class Preference extends Configured implements Tool {
    static class PreferenceMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[,]");
            if (strs.length==4){
                double hoby_value = 0.0;
                if ("showProduct".equals(strs[2])){//这样比较可避免空指针
                    hoby_value = 0.05;
                }else if ("addCart".equals(strs[2])){
                    hoby_value = 0.15;
                }else if ("createOrder".equals(strs[2])){
                    hoby_value = 0.3;
                }else if ("paySuccess".equals(strs[2])){
                    hoby_value = 0.5;
                }else {
                    hoby_value = 0.1;
                }
                context.write(new Text(strs[0]+","+strs[1]),new DoubleWritable(hoby_value));
            }
        }
    }

    static class PerferenceReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val:values){
                sum += val.get();
            }
            context.write(key,new DoubleWritable(sum));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
//        String Preference_output = conf.get("Preference_output");
        Path Preference_path = new Path("/user/hdfs/flume/shop_result/preference");//定死输出目录

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(Preference_path)){
            fs.delete(Preference_path,true);
        }

        Job job1 = Job.getInstance();
        job1.setJarByClass(Preference.class);
        job1.setJobName("Preference");

        job1.setMapperClass(PreferenceMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setReducerClass(PerferenceReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.addInputPath(job1,new Path(input));
        SequenceFileOutputFormat.setOutputPath(job1,Preference_path);
        return job1.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new Preference(),args));
    }

}
