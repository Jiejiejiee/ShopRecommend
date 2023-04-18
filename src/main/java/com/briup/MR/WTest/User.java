package com.briup.MR.WTest;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User implements Writable {
    private int id;
    private String name;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //对大数据存储值进行序列化
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.name = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

    public User() {
    }

    public User(int id, String name) {
        this.id = id;
        this.name = name;
    }//构建完有参构造器还应构造无参构造器

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
