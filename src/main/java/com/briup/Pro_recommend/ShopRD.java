package com.briup.Pro_recommend;

import com.briup.Pro_recommend.bean.ShopRecommend;
import com.briup.Pro_recommend.util.ShopGroup;
import com.briup.Pro_recommend.util.ShopID;
import com.briup.Pro_recommend.util.ShopPatitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.ShopRD -D input=/user/hdfs/flume/shop

 */
public class ShopRD extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
        Path Preference_path = new Path("/user/hdfs/flume/shop_result/preference");
        Path ProcessValue_path = new Path("/user/hdfs/flume/shop_result/processvalue");
        Path CoOccurrence_path = new Path("/user/hdfs/flume/shop_result/cooccurrence");
        Path StatisticalCo_path = new Path("/user/hdfs/flume/shop_result/statisticalco");
        Path MatrixCO_path = new Path("/user/hdfs/flume/shop_result/matrixco");
        Path RecommendValue_path = new Path("/user/hdfs/flume/shop_result/recommendvalue");
        Path AccumulateRV_path = new Path("/user/hdfs/flume/shop_result/accumulaterv");
        Path EliminateRV_path = new Path("/user/hdfs/flume/shop_result/eliminaterv");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(Preference_path)){
            fs.delete(Preference_path,true);
        }
        if (fs.exists(ProcessValue_path)){
            fs.delete(ProcessValue_path,true);
        }
        if (fs.exists(CoOccurrence_path)){
            fs.delete(CoOccurrence_path,true);
        }
        if (fs.exists(StatisticalCo_path)){
            fs.delete(StatisticalCo_path,true);
        }
        if (fs.exists(MatrixCO_path)){
            fs.delete(MatrixCO_path,true);
        }
        if (fs.exists(RecommendValue_path)){
            fs.delete(RecommendValue_path,true);
        }
        if (fs.exists(AccumulateRV_path)){
            fs.delete(AccumulateRV_path,true);
        }
        if (fs.exists(EliminateRV_path)){
            fs.delete(EliminateRV_path,true);
        }

        //step1
        Job job1 = Job.getInstance();
        job1.setJarByClass(Preference.class);
        job1.setJobName("Preference");

        job1.setMapperClass(Preference.PreferenceMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setReducerClass(Preference.PerferenceReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.addInputPath(job1,new Path(input));
        SequenceFileOutputFormat.setOutputPath(job1,Preference_path);

        //step2
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(ProcessValue.class);
        job2.setJobName("ProcessValue");

        job2.setMapperClass(ProcessValue.ProcessValueMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setReducerClass(ProcessValue.ProcessValueReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job2,Preference_path);
        SequenceFileOutputFormat.setOutputPath(job2,ProcessValue_path);

        //step3
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(CoOccurrence.class);
        job3.setJobName("CoOccurrence");

        job3.setMapperClass(CoOccurrence.CoOccurrenceMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(CoOccurrence.CoOccurrenceReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job3,Preference_path);
        SequenceFileOutputFormat.setOutputPath(job3,CoOccurrence_path);

        //step4
        Job job4 = Job.getInstance(conf);
        job4.setJarByClass(StatisticalCO.class);
        job4.setJobName("StatisticalCO");

        job4.setMapperClass(StatisticalCO.StatisticalCoMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(IntWritable.class);

        job4.setReducerClass(IntSumReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job4,CoOccurrence_path);
        SequenceFileOutputFormat.setOutputPath(job4,StatisticalCo_path);

        //step5
        Job job5 = Job.getInstance(conf);
        job5.setJarByClass(MatrixCO.class);
        job5.setJobName("MatrixCO");

        job5.setMapperClass(MatrixCO.MatrixCOMapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setReducerClass(MatrixCO.MatrixCOReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job5,StatisticalCo_path);
        SequenceFileOutputFormat.setOutputPath(job5,MatrixCO_path);

        //step6
        Job job6 = Job.getInstance(conf);
        job6.setJarByClass(RecommendValue.class);
        job6.setJobName("RecommendValue");

        job6.setMapperClass(RecommendValue.RecommendValueMapper.class);
        job6.setMapOutputKeyClass(ShopID.class);
        job6.setMapOutputValueClass(Text.class);

        job6.setPartitionerClass(ShopPatitioner.class);
        job6.setGroupingComparatorClass(ShopGroup.class);

        job6.setReducerClass(RecommendValue.RecommendValueReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(DoubleWritable.class);

        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job6,ProcessValue_path);
        SequenceFileInputFormat.addInputPath(job6,MatrixCO_path);
        SequenceFileOutputFormat.setOutputPath(job6,RecommendValue_path);

        //step7
        Job job7 = Job.getInstance(conf);
        job7.setJarByClass(AccumulateRV.class);
        job7.setJobName("AccumulateRV");

        job7.setMapperClass(AccumulateRV.AccumulateRVMapper.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(DoubleWritable.class);

        job7.setReducerClass(AccumulateRV.AccumulateRVReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(DoubleWritable.class);

        job7.setInputFormatClass(SequenceFileInputFormat.class);
        job7.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job7,RecommendValue_path);
        SequenceFileOutputFormat.setOutputPath(job7,AccumulateRV_path);

        //step8
        Job job8 = Job.getInstance(conf);
        job8.setJarByClass(EliminateRV.class);
        job8.setJobName("EliminateRV");

        MultipleInputs.addInputPath(job8,new Path(input), TextInputFormat.class, EliminateRV.LogerMapper.class);
        MultipleInputs.addInputPath(job8,AccumulateRV_path, SequenceFileInputFormat.class, EliminateRV.EliminateRVMapper.class);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(DoubleWritable.class);

        job8.setReducerClass(EliminateRV.EliminateRVReducer.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(DoubleWritable.class);

        job8.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job8,EliminateRV_path);

        //step9
        Job job9 = Job.getInstance(conf);
        job9.setJarByClass(WriteToDb.class);
        job9.setJobName("WriteToDb");

        DBConfiguration.configureDB(job9.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/shop",
                "root",
                "root");
        DBOutputFormat.setOutput(job9, " t_recommend_shop", "user_id","shops_id","recommand_value");

        job9.setMapperClass(WriteToDb.WriteToDbMapper.class);
        job9.setMapOutputKeyClass(Text.class);
        job9.setMapOutputValueClass(DoubleWritable.class);

        job9.setReducerClass(WriteToDb.WriteToDbReducer.class);
        job9.setOutputKeyClass(ShopRecommend.class);
        job9.setOutputValueClass(NullWritable.class);

        job9.setInputFormatClass(SequenceFileInputFormat.class);
        job9.setOutputFormatClass(DBOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job9,EliminateRV_path);

        //构建可控job作业
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);
        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);
        cj2.addDependingJob(cj1);
        ControlledJob cj3 = new ControlledJob(conf);
        cj3.setJob(job3);
        cj3.addDependingJob(cj1);
        ControlledJob cj4 = new ControlledJob(conf);
        cj4.setJob(job4);
        cj4.addDependingJob(cj3);
        ControlledJob cj5 = new ControlledJob(conf);
        cj5.setJob(job5);
        cj5.addDependingJob(cj4);
        ControlledJob cj6 = new ControlledJob(conf);
        cj6.setJob(job6);
        cj6.addDependingJob(cj2);
        cj6.addDependingJob(cj5);
        ControlledJob cj7 = new ControlledJob(conf);
        cj7.setJob(job7);
        cj7.addDependingJob(cj6);
        ControlledJob cj8 = new ControlledJob(conf);
        cj8.setJob(job8);
        cj8.addDependingJob(cj7);
        ControlledJob cj9 = new ControlledJob(conf);
        cj9.setJob(job9);
        cj9.addDependingJob(cj8);

        //构建作业流
        JobControl jc = new JobControl("ShopRD");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);
        jc.addJob(cj8);
        jc.addJob(cj9);

        Thread t = new Thread(jc);
        t.start();
        while (true){
            for (ControlledJob c:jc.getRunningJobList()){
                c.getJob().monitorAndPrintJob();
            }
            if (jc.allFinished())break;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new ShopRD(), args));
    }
}
