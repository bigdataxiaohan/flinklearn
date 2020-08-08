/**
 * @Classname RabbitMQProduce
 * @Description TODO
 * @Date 2020/7/19 14:18
 * @Created by hph
 */

package com.hph.datasource.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQPublisher {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();

        //设置RabbitMQ相关信息
        factory.setHost("hadoop102");
        factory.setUsername("test");
        factory.setPassword("123456");
        factory.setPort(5672);
        //创建连接
        Connection connection = factory.newConnection();
        //创建通道
        Channel channel = connection.createChannel();
        String message = "MQ FROM Java Client-";
        int i=0;
        while (true){
            channel.basicPublish("", "Test", null, (message + i).getBytes("UTF-8"));
            Thread.sleep((int)(Math.random()*1000));
            i++;
            System.out.println(message + i);
        }
    }
}
