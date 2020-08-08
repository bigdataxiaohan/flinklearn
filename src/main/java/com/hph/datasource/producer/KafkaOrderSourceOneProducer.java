/**
 * @Classname KafkaOrderProducer
 * @Description TODO
 * @Date 2020/7/30 15:59
 * @Created by hph
 */

package com.hph.datasource.producer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hph.bean.OrderBean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaOrderSourceOneProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
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
        int i = 0;
        while (true) {
            OrderBean orderBean = new OrderBean();
            orderBean.setProvinceCode(10000);
            orderBean.setCityCode(10000);
            orderBean.setUserId("OneUid-" + i);
            orderBean.setMoney((double) i);
            Gson gson = new GsonBuilder().create();
            String orderBeanJson = gson.toJson(orderBean);
            producer.send(new ProducerRecord<String, String>("Union_topic_1", orderBeanJson));
            System.out.println(orderBeanJson);
            i++;
            Thread.sleep(5000);
        }
    }
}
