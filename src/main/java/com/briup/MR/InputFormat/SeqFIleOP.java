package com.briup.MR.InputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.MR.InputFormat.SeqFIleOP -D input=/user/hdfs/db.txt  -D output=/user/hdfs/seq_w

 */
public class SeqFIleOP extends Configured implements Tool {
    static class SeqFIleOPMapper extends Mapper<Text, Text,IntWritable,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(Integer.parseInt(key.toString())),value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

        Job job=Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("SeqWrite");

        job.setMapperClass(SeqFIleOPMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        KeyValueTextInputFormat.addInputPath(job,new Path(input));
        //SequenceFileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        //SequenceFileOutputFormat.setOutputCompressionType(job,
        //SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(
                new ToolRunner().run(
                        new SeqFIleOP(),args));
    }
}