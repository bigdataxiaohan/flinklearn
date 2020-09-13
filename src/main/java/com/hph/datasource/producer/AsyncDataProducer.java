package com.hph.datasource.producer;


import java.math.BigDecimal;

import com.apifan.common.random.source.DateTimeSource;
import com.apifan.common.random.source.OtherSource;
import com.google.gson.Gson;
import com.hph.bean.UserLocation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.util.Properties;

public class AsyncDataProducer {


    //以济南市范围为例
    //北 117.033334,36.685959
    // 南 117.029382,36.667087
    // 西 117.017668,36.673745
    // 东 117.043826,36.67554

    private static double chinaWest = 115.7958606;
    private static double chinaEast = 117.83072785;
    private static double chinaSouth = 37.89179029;
    private static double chinaNorth = 42.36503919;
    private static Gson gson;

    //117.033945,36.684657

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


        String user = OtherSource.getInstance().randomPlateNumber(true);
        LocalDateTime begin = LocalDateTime.of(2020, 3, 12, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2020, 9, 12, 23, 30, 0);
        gson = new Gson();
        for (int i = 0; i <= 300; i++) {
            long timestamp = DateTimeSource.getInstance().randomTimestamp(begin, end) / 1000;
            UserLocation userLocation = new UserLocation(user, timestamp, randomLonLat(true), randomLonLat(false), "");
            System.out.println(userLocation);
            producer.send(new ProducerRecord<String, String>("car_location", gson.toJson(userLocation)));
        }
    }

    private static String randomLonLat(boolean isLongitude) {
        if (isLongitude) {
            BigDecimal bigDecimal = new BigDecimal(Math.random() * (chinaEast - chinaWest) + chinaWest);
            return bigDecimal.setScale(5, BigDecimal.ROUND_HALF_UP).toString();
        } else {
            BigDecimal bigDecimal = new BigDecimal(Math.random() * (chinaNorth - chinaSouth) + chinaSouth);
            return bigDecimal.setScale(6, BigDecimal.ROUND_HALF_UP).toString();
        }
    }


}
