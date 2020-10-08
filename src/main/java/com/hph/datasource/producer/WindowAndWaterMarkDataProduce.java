package com.hph.datasource.producer;

import com.apifan.common.random.source.OtherSource;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hph.bean.MallOrder;
import com.hph.bean.OrderBean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

public class WindowAndWaterMarkDataProduce {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 10);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Gson gson = new GsonBuilder().create();

        int cnt = 0;
        //2020-09-20 08:02:58           2020-09-20 09:02:58
        for (int i = 1600560178; i < 1600563778; i += 1) {
            MallOrder mallOrder = new MallOrder();
            mallOrder.setUserId("UID" + cnt);
            mallOrder.setTimeStamp(i);
            if (i % 2 == 0) {
                mallOrder.setItemId(0);
            } else {
                mallOrder.setItemId(1);
            }
            mallOrder.setMoneyCost(cnt);
            String mallOrderJson = gson.toJson(mallOrder);
            producer.send(new ProducerRecord<String, String>("MallOrder", mallOrderJson));
            cnt++;
            Thread.sleep(10);
        }
        System.out.println("总数据条数为" + cnt);

    }
}
