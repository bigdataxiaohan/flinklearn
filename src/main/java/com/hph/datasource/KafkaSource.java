/**
 * @Classname KafkaSource
 * @Description TODO
 * @Date 2020/7/19 16:21
 * @Created by hph
 */

package com.hph.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        //指定kafka的Broker地址
        props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //设置组ID
        props.setProperty("group.id","flink-comsumer");
        props.setProperty("auto.offset.reset","earliest");
        //kafka自动提交偏移量，
        props.setProperty("enable.auto.commit","false");

        FlinkKafkaConsumer<String> kafkaSouce = new FlinkKafkaConsumer<>("avro_test",
                new SimpleStringSchema(),
                props);
        DataStreamSource<String> kafkaSource = environment.addSource(kafkaSouce);
        kafkaSource.print();
        environment.execute("");
    }
}
