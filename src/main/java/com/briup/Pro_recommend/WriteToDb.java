package com.briup.Pro_recommend;

import com.briup.Pro_recommend.bean.ShopRecommend;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
Step9
text                doublewritable
4:10003          8.049999999999999
map原样输出，reduce封装
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.WriteToDb

 */
public class WriteToDb extends Configured implements Tool {
    static class WriteToDbMapper extends Mapper<Text, DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    static class WriteToDbReducer extends Reducer<Text,DoubleWritable, ShopRecommend, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String[] userid_shopid = key.toString().split(":");
            for (DoubleWritable val:values){
                ShopRecommend sr = new ShopRecommend(Long.parseLong(userid_shopid[0]),Long.parseLong(userid_shopid[1]),val.get());
                context.write(sr,NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path EliminateRV_path = new Path("/user/hdfs/flume/shop_result/eliminaterv");

        Job job9 = Job.getInstance(conf);
        job9.setJarByClass(WriteToDb.class);
        job9.setJobName("WriteToDb");

        DBConfiguration.configureDB(job9.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/shop",
                "root",
                "root");
        DBOutputFormat.setOutput(job9, " t_recommend_shop", "user_id","shops_id","recommand_value");

        job9.setMapperClass(WriteToDbMapper.class);
        job9.setMapOutputKeyClass(Text.class);
        job9.setMapOutputValueClass(DoubleWritable.class);

        job9.setReducerClass(WriteToDbReducer.class);
        job9.setOutputKeyClass(ShopRecommend.class);
        job9.setOutputValueClass(NullWritable.class);

        job9.setInputFormatClass(SequenceFileInputFormat.class);
        job9.setOutputFormatClass(DBOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job9,EliminateRV_path);
        return job9.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new WriteToDb(),args));
    }
}
