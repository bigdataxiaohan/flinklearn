package com.hph.sink;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.text.DecimalFormat;
import java.util.Properties;

public class RedisSink {
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


        //添加kafka数据源
        DataStreamSource<String> order = environment.addSource(new FlinkKafkaConsumer<>(
                "Order_Reduce",
                new SimpleStringSchema(),
                props
        ));

        //映射为Orderbean
        SingleOutputStreamOperator<OrderBean> orderBean = order.map(new MapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String value) throws Exception {
                Gson gson = new Gson();
                OrderBean orderBean = gson.fromJson(value, OrderBean.class);
                return orderBean;
            }
        });


        //分组求和
        SingleOutputStreamOperator<OrderBean> sumed = orderBean.keyBy(new KeySelector<OrderBean, Integer>() {
            @Override
            public Integer getKey(OrderBean value) throws Exception {
                return value.getProvinceCode();
            }
        }).sum("money");

        //选择 k v 写入redis
        SingleOutputStreamOperator<Tuple2<Integer, Double>> result = sumed.map(new MapFunction<OrderBean, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> map(OrderBean value) throws Exception {
                int provinceCode = value.getProvinceCode();
                Double money = value.getMoney();
                return Tuple2.of(provinceCode, money);
            }
        });


        //创建Redis链接
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).setDatabase(0).build();


        //将数据写入Reids redis
        result.addSink(new org.apache.flink.streaming.connectors.redis.RedisSink<Tuple2<Integer, Double>>(conf, new RedisActivityBeanMapper()));

        environment.execute("Redis Sink");

    }

    private static class RedisActivityBeanMapper implements RedisMapper<Tuple2<Integer, Double>> {
        //解决科学计数法的问题
        DecimalFormat df = new DecimalFormat("#");

        //用哪里中操作方法将数据写入
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "province_order_cnt");
        }

        @Override
        public String getKeyFromData(Tuple2<Integer, Double> data) {
            return String.valueOf(data.f0);
        }

        @Override
        public String getValueFromData(Tuple2<Integer, Double> data) {

            return (df.format(data.f1));
        }

    }
}
