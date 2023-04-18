package com.briup.MR.WTest;

import com.briup.MR.WTest.User;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class ArrayWritableTest {
    public static void main(String[] args) {

//        String[] str = {"lisi","tom","jack"};
//        ArrayWritable aw = new ArrayWritable(str);
//        Writable[] n = aw.get();
////        for (Writable n1:n) {
////            System.out.println(n1.toString());
////        }
//        String[] s = aw.toStrings();
//        for (String s1:s){
//            System.out.println(s1.toString());
//        }


//        IntWritable[] arr = {new IntWritable(1),new IntWritable(2)};//构建大数据类型数组
//        ArrayWritable a = new ArrayWritable(IntWritable.class);
//        a.set(arr);
////        System.out.println(Arrays.toString(a.toStrings()));
//        for (Writable n1:a.get()) {
//            IntWritable n2 = (IntWritable) n1;//强制性转换大数据类型
//            System.out.println(n2.get());
//        }

        //自定义的数组类型
        User[] users = {new User(1,"jack"),new User(2,"tom")};
        ArrayWritable a = new ArrayWritable(User.class,users);
        User[] aw = (User[])a.get();
        for (User u1:aw){
            System.out.println(u1);
        }

    }

}
