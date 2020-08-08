/**
 * @Classname SplitDemo
 * @Description TODO
 * @Date 2020/8/8 19:04
 * @Created by hph
 */

package com.hph.transformation;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SelectDemo {

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

        DataStreamSource<String> order = environment.addSource(new FlinkKafkaConsumer<>(
                "Order_Reduce",
                new SimpleStringSchema(),
                props
        ));
        SplitStream<String> split = order.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                OrderBean orderBean = new Gson().fromJson(value, OrderBean.class);
                if (orderBean.getCityCode() == 10000) {
                    return Arrays.asList("10000");
                } else {
                    return Arrays.asList("37001");
                }
            }
        });
        split.select("10000").print();
        environment.execute("SelectDemo");
    }
}
