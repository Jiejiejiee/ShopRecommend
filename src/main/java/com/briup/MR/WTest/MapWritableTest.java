package com.briup.MR.WTest;

import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Map;

public class MapWritableTest {
    public static void main(String[] args) {
        MapWritable aw = new MapWritable();
        aw.put(new IntWritable(1),new Text("test1"));
        aw.put(new IntWritable(2),new Text("test2"));
        aw.put(new IntWritable(3),new Text("test3"));
        aw.put(new IntWritable(4),new Text("test4"));
        aw.put(new IntWritable(5),new Text("test5"));
        //System.out.println(aw.size());
//        aw.forEach((x,y)-> System.out.println(x+"-"+y));
//        System.out.println(aw.get(new IntWritable(2)));
//        Text v = (Text) aw.get(new IntWritable(2));
//        System.out.println(v.toString());
//        System.out.println(aw.containsKey(new IntWritable(3)));//其中是否包含1键值
//        System.out.println(aw.containsValue(new Text("test6")));//其中是否包含test6键值
//        for (Map.Entry<Writable,Writable> entry:aw.entrySet()){//参数只能设置为Writable类型，可在循环体中更改类型
//            IntWritable ak = (IntWritable) entry.getKey();
//            Text av = (Text) entry.getValue();
//            System.out.println(ak+"-"+av);
//        }
//        aw.remove(new IntWritable(3));//移除键值
//        aw.clear();//清空键值
        Map<IntWritable,Text> map = new HashMap<>();
        map.put(new IntWritable(6),new Text("test6"));
        aw.putAll(map);//将aw中的数据放入map中
        aw.forEach((x,y)-> System.out.println(x+"-"+y));
    }
}
