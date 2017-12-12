package com.spark.demo.NetworkDemo;

import com.google.common.io.Closeables;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * socket 读取客户端输入
 */
public class SocketCustomReceive extends Receiver<String> {
    // ============= Receiver code that receives data over a socket ==============
    private String host;
    private int port = -1;

    public SocketCustomReceive(String host, int port) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.port = port;
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false
    }

    public void receive(){

        try {
            Socket socket = null;
            BufferedReader reader = null;
            try {
                socket = new Socket(host,port);
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                String userInput;
                while (!isStopped() && (userInput = reader.readLine()) != null){
                    System.out.println("Received message : "+ userInput + " --");
                    store(userInput);
                }
            }finally {
                Closeables.close(reader,true);
                Closeables.close(socket,true);
            }
        }
        catch (ConnectException ce){
            restart("Can not connect to socket " + ce);
        }catch(Throwable throwable){
            restart("Error receiving data " + throwable);
        }
    }
// ============= Receiver code that receives data over a socket ==============
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SocketReceived").setMaster("spark://HOST1:PORT1,HOST2:PORT2");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(2000));

        JavaInputDStream<String> lines = context.receiverStream(new SocketCustomReceive("host",8080));



    }

}
