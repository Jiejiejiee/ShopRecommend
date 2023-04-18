package com.briup.MR.InputFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//mr读取数据库，DBinputformat读取数据库表的一行，转换为一组键值对
//键为表中的行号，值为封装的对象
public class Teacher implements WritableComparable<Teacher>, DBWritable {
    private int id;
    private String name;
    private int age;

    public Teacher() {
    }

    public Teacher(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Teacher(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

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

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Teacher{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();//id
        this.name = dataInput.readUTF();//name
        this.age = dataInput.readInt();//age
    }

    //sql:insert into teacher(id,name,age) values(?,?,?)
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1,this.id);
        preparedStatement.setString(2,this.name);
        preparedStatement.setInt(3,this.age);
    }

    //select id,name,age from teacher;
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt("id");
        this.name = resultSet.getString("name");
        this.age = resultSet.getInt("age");
    }


    @Override
    public int compareTo(Teacher o) {
        return this.getAge()-o.getAge();
    }
}
