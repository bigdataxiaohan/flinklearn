/**
 * @Classname ConnectDemo
 * @Description TODO
 * @Date 2020/8/8 18:05
 * @Created by hph
 */

package com.hph.transformation;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        //指定kafka的Broker地址
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //设置组ID
        props.setProperty("group.id", "flink");
        props.setProperty("auto.offset.reset", "earliest");
        //kafka自动提交偏移量，
        props.setProperty("enable.auto.commit", "false");
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        DataStreamSource<String> topicOne = environment.addSource(new FlinkKafkaConsumer<>(
                "Union_topic_1",
                new SimpleStringSchema(),
                props
        ));
        DataStreamSource<String> topicTwo = environment.addSource(new FlinkKafkaConsumer<>(
                "Union_topic_2",
                new SimpleStringSchema(),
                props
        ));

        ConnectedStreams<String, String> connect = topicOne.connect(topicTwo);

        SingleOutputStreamOperator<OrderBean> map = connect.map(new CoMapFunction<String, String, OrderBean>() {
            @Override
            public OrderBean map1(String value) throws Exception {
                OrderBean orderBean = new Gson().fromJson(value, OrderBean.class);
                return orderBean;
            }
            @Override
            public OrderBean map2(String value) throws Exception {
                OrderBean orderBean = new Gson().fromJson(value, OrderBean.class);
                return orderBean;
            }
        });

        map.print();
        environment.execute("ConnectionDemo");
    }
}
