package com.briup.MR.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public class SeqReader {
    public static void main(String[] args) throws IOException, IllegalAccessException, InstantiationException {
        Configuration conf = new Configuration();
        conf.set("fs.dafaultFS","hdfs://192.168.196.128:9000");
        Path path = new Path("hdfs://192.168.196.128:9000/user/hdfs/flume/shop_result/eliminaterv/part-r-00000");
        SequenceFile.Reader.Option op = SequenceFile.Reader.file(path);
        SequenceFile.Reader sr = new SequenceFile.Reader(conf,op);
        Writable key = (Writable) sr.getKeyClass().newInstance();
        Writable val = (Writable) sr.getValueClass().newInstance();
        while (sr.next(key,val)){
            System.out.println(key+"          "+val);
        }
    }
}
