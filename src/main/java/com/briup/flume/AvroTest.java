package com.briup.flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;
import java.util.Currency;
import java.util.HashMap;
import java.util.Map;

//给avro source的flume发数据，avro source要求获取的是事件对象
public class AvroTest {
    public static void main(String[] args) throws EventDeliveryException,InterruptedException {
        //构建Rpc客户端，发送数据
        RpcClient client= RpcClientFactory.getDefaultInstance("192.168.196.128",8888);
        //构建event事件对象中的header属性键值
        Map<String,String> header_attr=new HashMap<String,String>();
        header_attr.put("name","test1");
        header_attr.put("body","test2");
        //构建事件对象
        Event event=EventBuilder.withBody("AvroTest...briup", Charset.forName("utf8"),header_attr);
        client.append(event);
        Thread.sleep(2000);
        if(client!=null)client.close();

    }
}
