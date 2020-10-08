package com.hph.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 广播流读取Redis
 */

public class BroadcastStreamStream {

    public static void main(String[] args) throws Exception {

        // 构建流处理环境
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置处理环境的并发度为4
        environment.setParallelism(4);
        final MapStateDescriptor<String, String> REDIS_BROADCAST = new MapStateDescriptor<>(
                "redis-broadCast",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        // 自定义广播流（单例）
        BroadcastStream<Map<String, String>> broadcastStream = environment.addSource(new RichSourceFunction<Map<String, String>>() {

            private volatile boolean isRunning = true;
            private volatile Jedis jedis;
            private volatile Map<String, String> map = new ConcurrentHashMap(16);

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                jedis = new Jedis("hadoop102", 6379);

            }

            /**
             * 数据源：模拟每30秒重新读取redis中的相关数据
             * @param ctx
             * @throws Exception
             */

            @Override
            public void run(SourceContext<Map<String, String>> ctx) throws Exception {
                while (isRunning) {
                    long newTime = System.currentTimeMillis();
                    //每间隔30秒查询一次Redis的数据作为广播变量
                    TimeUnit.SECONDS.sleep(30);
                    map = jedis.hgetAll("broadcast");
                    ctx.collect(map);
                    map.clear();
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).setParallelism(1).broadcast(REDIS_BROADCAST);


        //生成测试数据
        DataStreamSource<Long> data = environment.generateSequence(1, 891);
        DataStream<String> dataStream = data.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                //数据每间隔5s发送一次
                Thread.sleep(5000);
                return String.valueOf(value);
            }
        });

        // 数据流和广播流连接处理并将拦截结果打印
        dataStream.connect(broadcastStream).process(new BroadcastProcessFunction<String, Map<String, String>, String>() {
            private volatile int i;
            private volatile Map<String, String> keywords;
            private volatile Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                if (keywords == null) {
                    jedis = new Jedis("hadoop102", 6379);
                    Map<String, String> resultMap = jedis.hgetAll("broadcast");
                    keywords = resultMap;
                }
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<String> out) throws Exception {
                //todo 获取的Map的数据
                keywords = value;

            }

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                out.collect("数据源中的ID:" + value + ", 广播变量中获取到Value：" + keywords.get(value));
            }

        }).print();

        environment.execute();
    }

}