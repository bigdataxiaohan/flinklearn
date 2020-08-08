/**
 * @Classname CustomPartion
 * @Description TODO
 * @Date 2020/8/8 22:55
 * @Created by hph
 */

package com.hph.transformation;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import com.hph.datasource.producer.MyRedisSource;
import com.sun.org.apache.regexp.internal.RE;
import com.sun.org.apache.xpath.internal.operations.Or;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.Random;

public class CustomPartion {
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
        System.out.println(order.getParallelism());

        DataStream<String> partitionCustom = order.partitionCustom(new MyCustomerPartition(), new KeySelector<String, OrderBean>() {
            @Override
            public OrderBean getKey(String value) throws Exception {
                OrderBean orderBean = new Gson().fromJson(value, OrderBean.class);
                return orderBean;
            }
        });
        partitionCustom.print();
        System.out.println("自定义分区"+partitionCustom.getParallelism());
        environment.execute("CustomPartion");

    }


    public static class MyCustomerPartition implements Partitioner<OrderBean> {
        @Override
        public int partition(OrderBean key, int numPartitions) {
            if (key.getCityCode() == 10000) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}

