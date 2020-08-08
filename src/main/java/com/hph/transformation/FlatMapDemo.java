/**
 * @Classname FlatMapDemo
 * @Description TODO
 * @Date 2020/8/8 15:50
 * @Created by hph
 */

package com.hph.transformation;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        //指定kafka的Broker地址
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //设置组ID
        props.setProperty("group.id", "flink-comsumer");
        props.setProperty("auto.offset.reset", "earliest");
        //kafka自动提交偏移量，
        props.setProperty("enable.auto.commit", "false");
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        DataStreamSource<String> order = environment.addSource(new FlinkKafkaConsumer<>(
                "Order",
                new SimpleStringSchema(),
                props
        ));

        SingleOutputStreamOperator<OrderBean> orderBeanDS = order.map(new MapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String value) throws Exception {
                return new Gson().fromJson(value, OrderBean.class);
            }
        });

        SingleOutputStreamOperator<OrderBean> flatMapDS = orderBeanDS.flatMap(new FlatMapFunction<OrderBean,
                OrderBean>() {
            @Override
            public void flatMap(OrderBean value, Collector<OrderBean> out) throws Exception {
                if (value.getMoney() % 2 == 0) {
                    out.collect(value);
                }
            }
        });

        flatMapDS.print();
        environment.execute("FlatMapDemo");

    }
}
