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
/*
Step7
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.AccumulateRV

 */
public class AccumulateRV extends Configured implements Tool {
    static class AccumulateRVMapper extends Mapper<Text, DoubleWritable,Text,DoubleWritable> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    static class AccumulateRVReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum_rem = 0;
            for (DoubleWritable val:values){
                sum_rem += val.get();
            }
            context.write(key,new DoubleWritable(sum_rem));
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path RecommendValue_path = new Path("/user/hdfs/flume/shop_result/recommendvalue");
        Path AccumulateRV_path = new Path("/user/hdfs/flume/shop_result/accumulaterv");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(AccumulateRV_path)){
            fs.delete(AccumulateRV_path,true);
        }

        Job job7 = Job.getInstance(conf);
        job7.setJarByClass(AccumulateRV.class);
        job7.setJobName("AccumulateRV");

        job7.setMapperClass(AccumulateRVMapper.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(DoubleWritable.class);

        job7.setReducerClass(AccumulateRVReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(DoubleWritable.class);

        job7.setInputFormatClass(SequenceFileInputFormat.class);
        job7.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job7,RecommendValue_path);
        SequenceFileOutputFormat.setOutputPath(job7,AccumulateRV_path);
        return job7.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new AccumulateRV(),args));
    }
}
