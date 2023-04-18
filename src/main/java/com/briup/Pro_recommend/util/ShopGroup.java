package com.briup.Pro_recommend.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ShopGroup extends WritableComparator {
    public ShopGroup(){
        super(ShopID.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ShopID a1 = (ShopID) a;
        ShopID b1 = (ShopID) b;
        return a1.getShopID().compareTo(b1.getShopID());
    }
}
