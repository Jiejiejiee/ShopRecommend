package com.briup.Pro_recommend;

import com.sun.jersey.core.impl.provider.entity.SourceProvider;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/*
Step8
对AccumulateRV中的数据进行清洗，将已购买的商品的推荐值剔除,value=1
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.EliminateRV -D input=/user/hdfs/flume/shop


 */
public class EliminateRV extends Configured implements Tool {
    static class LogerMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            if ("paySuccess".equals(strs[2].trim())){
                context.write(new Text(strs[0]+":"+strs[1]),new DoubleWritable(1));
            }
        }
    }

    static class EliminateRVMapper extends Mapper<Text, DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    static class EliminateRVReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> iter = values.iterator();
            double val = iter.next().get();
            //迭代器，如果value中没有下一个元素，输出
            if (!iter.hasNext()){
                context.write(key,new DoubleWritable(val));
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
        Path AccumulateRV_path = new Path("/user/hdfs/flume/shop_result/accumulaterv");
        Path EliminateRV_path = new Path("/user/hdfs/flume/shop_result/eliminaterv");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(EliminateRV_path)){
            fs.delete(EliminateRV_path,true);
        }

        Job job8 = Job.getInstance(conf);
        job8.setJarByClass(EliminateRV.class);
        job8.setJobName("EliminateRV");

        //第一个map和原始文件对应
        MultipleInputs.addInputPath(job8,new Path(input), TextInputFormat.class,LogerMapper.class);
        //读取第七步结果
        MultipleInputs.addInputPath(job8,AccumulateRV_path, SequenceFileInputFormat.class,EliminateRVMapper.class);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(DoubleWritable.class);

        job8.setReducerClass(EliminateRVReducer.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(DoubleWritable.class);

        job8.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job8,EliminateRV_path);
        return job8.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new EliminateRV(),args));
    }
}
