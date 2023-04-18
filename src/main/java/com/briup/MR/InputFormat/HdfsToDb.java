package com.briup.MR.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
//写入数据库使用Dboutputformat，写入数据库reduce的key为写入对象，如果没有reduce那么map输出的键是写入对象
/*
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.MR.InputFormat.HdfsToDb -D input=/user/hdfs/mapreduce/hdfstodb/db.txt
 */
public class HdfsToDb extends Configured implements Tool {
    //读数据Textinputformat
    static class HdfsToDbMapper extends Mapper<LongWritable, Text, Teacher, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs[] = value.toString().split(",");
            Teacher tea = new Teacher(Integer.parseInt(strs[0]),strs[1],Integer.parseInt(strs[2]));
            context.write(tea,NullWritable.get());
        }
    }

    static class HdfsToDbReducer extends Reducer<Teacher,NullWritable,Teacher,NullWritable>{
        @Override
        protected void reduce(Teacher key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("HdfsToDb");

        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/mr",
                "root",
                "root");
        DBOutputFormat.setOutput(job, "teacher", "id", "name", "age");

        job.setMapperClass(HdfsToDbMapper.class);
        job.setMapOutputKeyClass(Teacher.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(HdfsToDbReducer.class);
        job.setOutputKeyClass(Teacher.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(input));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new HdfsToDb(), args));
    }
}
