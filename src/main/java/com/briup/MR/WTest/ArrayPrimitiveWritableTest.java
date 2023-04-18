package com.briup.MR.WTest;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.Arrays;

public class ArrayPrimitiveWritableTest {
    public static void main(String[] args) {
        int[] n = {1,2,3,4,5};
//        System.out.println(n.getClass());
//        ArrayPrimitiveWritable aw = new ArrayPrimitiveWritable();

//        ArrayPrimitiveWritable aw = new ArrayPrimitiveWritable(int.class);//申明模板的类
//        aw.set(n);

        ArrayPrimitiveWritable aw = new ArrayPrimitiveWritable(n);
        int[] res = (int[]) aw.get();
        System.out.println(Arrays.toString(res));




    }
}
