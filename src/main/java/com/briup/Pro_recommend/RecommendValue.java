package com.briup.Pro_recommend;

import com.briup.Pro_recommend.util.ShopGroup;
import com.briup.Pro_recommend.util.ShopID;
import com.briup.Pro_recommend.util.ShopPatitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*
Step6
yarn jar ShopRecommend-1.0-SNAPSHOT.jar com.briup.Pro_recommend.RecommendValue

 */
public class RecommendValue extends Configured implements Tool {
    static class RecommendValueMapper extends Mapper<Text,Text, ShopID,Text>{
        private Text filename = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fs = (FileSplit) context.getInputSplit();
            String fn = fs.getPath().getParent().getName().trim();//获取step2和step5的目录和结果文件,去掉两个文件之间可能存在的空格
//            PrintWriter pw = new PrintWriter("/home/hdfs/sp.log");//map结果测试
            filename.set(fn);
//            pw.println("---"+fn);
//            pw.flush();
//            pw.close();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            ShopID s = new ShopID();
            if ("processvalue".equals(filename.toString())){
                s.setShopID(key.toString());
                s.setFlag(0);
            }else if ("matrixco".equals(filename.toString())){
                s.setShopID(key.toString());
                s.setFlag(1);
            }
            context.write(s,value);
        }
    }

    static class RecommendValueReducer extends Reducer<ShopID,Text,Text, DoubleWritable>{
        private List<String> list = new ArrayList<>();
        @Override
        protected void reduce(ShopID key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values){
                list.add(val.toString());
            }
            if (list.size()!=2)return;
            String[] users = list.get(0).split(",");//
            String[] shops = list.get(1).split(",");
            for (String user:users){
                String[] userid_hoby = user.split(":");
                double hoby_val = Double.parseDouble(userid_hoby[1]);
                for (String shop:shops){
                    String[] shopid_num = shop.split(":");
                    int num = Integer.parseInt(shopid_num[1]);
                    String t = userid_hoby[0]+":"+shopid_num[0];
                    Double recomm_val = hoby_val*num;
                    context.write(new Text(t),new DoubleWritable(recomm_val));
                }
            }
            list.clear();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path ProcessValue_path = new Path("/user/hdfs/flume/shop_result/processvalue");
        Path MatrixCO_path = new Path("/user/hdfs/flume/shop_result/matrixco");
        Path RecommendValue_path = new Path("/user/hdfs/flume/shop_result/recommendvalue");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(RecommendValue_path)){
            fs.delete(RecommendValue_path,true);
        }

        Job job6 = Job.getInstance(conf);
        job6.setJarByClass(RecommendValue.class);
        job6.setJobName("RecommendValue");

        job6.setMapperClass(RecommendValueMapper.class);
        job6.setMapOutputKeyClass(ShopID.class);
        job6.setMapOutputValueClass(Text.class);

        job6.setPartitionerClass(ShopPatitioner.class);
        job6.setGroupingComparatorClass(ShopGroup.class);

        job6.setReducerClass(RecommendValueReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(DoubleWritable.class);

        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job6,ProcessValue_path);
        SequenceFileInputFormat.addInputPath(job6,MatrixCO_path);
        SequenceFileOutputFormat.setOutputPath(job6,RecommendValue_path);
        return job6.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new RecommendValue(), args));
    }
}
