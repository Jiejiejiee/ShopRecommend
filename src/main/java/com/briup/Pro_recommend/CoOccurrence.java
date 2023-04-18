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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/*
Step3
计算用户偏好商品两两之间的关系
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.CoOccurrence

 */
public class CoOccurrence extends Configured implements Tool {
    static class CoOccurrenceMapper extends Mapper<Text, DoubleWritable,Text,Text>{
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String strs[] = key.toString().split(",");
            context.write(new Text(strs[0]),new Text(strs[1]));
        }
    }

    static class CoOccurrenceReducer extends Reducer<Text,Text,Text,Text>{
        private List<String> list = new ArrayList<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values){
                list.add(val.toString());
            }
            for (String s:list){
                for (String s1:list){
                    context.write(new Text(s),new Text(s1));
                }
            }
            list.clear();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path Preference_path = new Path("/user/hdfs/flume/shop_result/preference");
        Path CoOccurrence_path = new Path("/user/hdfs/flume/shop_result/cooccurrence");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(CoOccurrence_path)){
            fs.delete(CoOccurrence_path,true);
        }

        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(CoOccurrence.class);
        job3.setJobName("CoOccurrence");

        job3.setMapperClass(CoOccurrenceMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(CoOccurrenceReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job3,Preference_path);
        SequenceFileOutputFormat.setOutputPath(job3,CoOccurrence_path);
        return job3.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new CoOccurrence(), args));
    }
}
