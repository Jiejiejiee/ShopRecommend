package com.briup.Pro_recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
Step2
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.ProcessValue

 */
public class ProcessValue extends Configured implements Tool {
    static class ProcessValueMapper extends Mapper<Text, DoubleWritable,Text,Text>{
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String[] ks = key.toString().split(",");
            context.write(new Text(ks[1]),new Text(ks[0]+":"+value.get()));
        }
    }

    static class ProcessValueReducer extends Reducer<Text,Text,Text,Text>{
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
        Path Preference_path = new Path("/user/hdfs/flume/shop_result/preference");
        Path ProcessValue_path = new Path("/user/hdfs/flume/shop_result/processvalue");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(ProcessValue_path)){
            fs.delete(ProcessValue_path,true);
        }

        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(ProcessValue.class);
        job2.setJobName("ProcessValue");

        job2.setMapperClass(ProcessValueMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setReducerClass(ProcessValueReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job2,Preference_path);
        SequenceFileOutputFormat.setOutputPath(job2,ProcessValue_path);
        return job2.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new ProcessValue(), args));
    }
}
