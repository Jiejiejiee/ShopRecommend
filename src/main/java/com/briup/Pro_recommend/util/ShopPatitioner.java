package com.briup.Pro_recommend.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ShopPatitioner extends Partitioner<ShopID, Text> {
    @Override
    public int getPartition(ShopID shopID, Text text, int numPatitions) {
        return Math.abs(shopID.getShopID().hashCode()*127%numPatitions);
    }
}
