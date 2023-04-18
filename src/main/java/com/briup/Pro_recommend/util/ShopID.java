package com.briup.Pro_recommend.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ShopID implements WritableComparable<ShopID> {
    private String shopID;
    //step5结果文件设为1,step2结果文件设为0
    private int flag;

    @Override
    public int compareTo(ShopID o) {
        int n = this.shopID.compareTo(o.getShopID());
        if (n==0){
            n = this.flag-o.flag;
        }
        return n;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(shopID);
        dataOutput.writeInt(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.shopID = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "ShopID{" +
                "shopID='" + shopID + '\'' +
                ", flag=" + flag +
                '}';
    }

    public ShopID() {
    }

    public ShopID(String shopID, int flag) {
        this.shopID = shopID;
        this.flag = flag;
    }

    public String getShopID() {
        return shopID;
    }

    public void setShopID(String shopID) {
        this.shopID = shopID;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
