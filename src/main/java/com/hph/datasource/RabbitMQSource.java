/**
 * @Classname RabbitMQSource
 * @Description TODO
 * @Date 2020/7/19 14:29
 * @Created by hph
 */

package com.hph.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQSource {
    public static void main(String[] args) throws Exception {
        //设置环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //RabbitMQ配置
        RMQConnectionConfig rmqConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("hadoop102")
                .setPort(5672)
                .setUserName("test")
                .setPassword("123456")
                .setVirtualHost("/")
                .build();
        //添加数据源
        DataStreamSource<String> rabbitMQSource = environment.addSource(new RMQSource<String>(
                rmqConnectionConfig,  //链接RabbitMQ配置信息
                "Test",    //
                true,
                new SimpleStringSchema()))
                .setParallelism(1);
        rabbitMQSource.print();

        environment.execute("Rabbit MQ Source");
    }
}

