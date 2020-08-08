/**
 * @Classname ProjectDemo
 * @Description TODO
 * @Date 2020/8/8 20:42
 * @Created by hph
 */

package com.hph.transformation;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ProjectDemo {
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
        SingleOutputStreamOperator<Tuple4<Integer, Integer, String, Double>> map = order.map(new MapFunction<String, Tuple4<Integer, Integer, String, Double>>() {
            @Override
            public Tuple4<Integer, Integer, String, Double> map(String value) throws Exception {
                OrderBean orderBean = new Gson().fromJson(value, OrderBean.class);
                int provinceCode = orderBean.getProvinceCode();
                int cityCode = orderBean.getCityCode();
                String userId = orderBean.getUserId();
                Double money = orderBean.getMoney();
                return Tuple4.of(provinceCode, cityCode, userId, money);
            }
        });

        map.project(2,3).print();
        environment.execute("Project");

    }
}
