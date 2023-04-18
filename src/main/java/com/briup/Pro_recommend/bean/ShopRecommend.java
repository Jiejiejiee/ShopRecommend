package com.briup.Pro_recommend.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ShopRecommend implements Writable, DBWritable {
    private long user_id;
    private long shops_id;
    private double recommand_value;

    public ShopRecommend() {
    }

    public ShopRecommend(long user_id, long shops_id, double recommand_value) {
        this.user_id = user_id;
        this.shops_id = shops_id;
        this.recommand_value = recommand_value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(user_id);
        dataOutput.writeLong(shops_id);
        dataOutput.writeDouble(recommand_value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user_id = dataInput.readLong();
        this.shops_id = dataInput.readLong();
        this.recommand_value = dataInput.readDouble();
    }

    //insert into t_recommend_shop(user_id,shops_id,recommand_value) values(?,?,?)
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(1,user_id);
        preparedStatement.setLong(2,shops_id);
        preparedStatement.setDouble(3,recommand_value);
    }

    //select user_id,shops_id,recommand_value from t_recommend_shop
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.user_id = resultSet.getLong("user_id");
        this.shops_id = resultSet.getLong("shops_id");
        this.recommand_value = resultSet.getDouble("recommand_value");
    }

    @Override
    public String toString() {
        return "ShopRecommend{" +
                "user_id=" + user_id +
                ", shops_id=" + shops_id +
                ", recommand_value=" + recommand_value +
                '}';
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public long getShops_id() {
        return shops_id;
    }

    public void setShops_id(long shops_id) {
        this.shops_id = shops_id;
    }

    public double getRecommand_value() {
        return recommand_value;
    }

    public void setRecommand_value(double recommand_value) {
        this.recommand_value = recommand_value;
    }
}
