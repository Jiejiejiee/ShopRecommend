package com.briup.flume;

import javax.sound.sampled.Port;
import java.io.*;
import java.net.Socket;

public class NetCatTest {
    public static void main(String[] args) throws IOException {
        //构建TCP-IP协议客户端
        Socket socket=new Socket("192.168.196.128", 8888);
        OutputStream os= socket.getOutputStream();
        PrintWriter pw=new PrintWriter(os);
        pw.println("hello world");
        pw.flush();
        InputStream is=socket.getInputStream();
        InputStreamReader isr=new InputStreamReader(is);
        BufferedReader br=new BufferedReader(isr);
        System.out.println(br.readLine());
        if(br!=null)br.close();
        if(isr!=null)isr.close();
        if(is!=null)is.close();
        if(pw!=null)pw.close();
        if(os!=null)os.close();
        if(socket!=null)socket.close();

    }
}
