/**
 * @Classname KeyByDemo
 * @Description TODO
 * @Date 2020/8/8 16:12
 * @Created by hph
 */

package com.hph.transformation;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ReduceDemo {
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

        SingleOutputStreamOperator<OrderBean> orderBeanDS = order.map(new MapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String value) throws Exception {
                return new Gson().fromJson(value, OrderBean.class);
            }
        });


        KeyedStream<OrderBean, Integer> keyedStream = orderBeanDS.keyBy(new KeySelector<OrderBean, Integer>() {
            @Override
            public Integer getKey(OrderBean value) throws Exception {
                return value.getProvinceCode();
            }
        });

        SingleOutputStreamOperator<OrderBean> reduceDS = keyedStream.reduce(new ReduceFunction<OrderBean>() {
            @Override
            public OrderBean reduce(OrderBean value1, OrderBean value2) throws Exception {
                OrderBean orderBean = new OrderBean();
                if (value1.getCityCode() == value2.getCityCode()) {
                    orderBean.setProvinceCode(value1.getProvinceCode());
                    orderBean.setCityCode(value1.getCityCode());
                    orderBean.setMoney(value1.getMoney() + value2.getMoney());
                }
                return orderBean;
            }
        });

        reduceDS.print();


        environment.execute("keyedDemo");

    }
}
