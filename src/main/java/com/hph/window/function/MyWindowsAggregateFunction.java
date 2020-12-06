package com.hph.window.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class MyWindowsAggregateFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> dataStreamSource = environment.addSource(new RichSourceFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

            }

            boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //随机产生数据为 A-Z附带时间戳
                while (isRunning) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Thread.sleep(1000);
                    char i = (char) (65 + new Random().nextInt(25));
                    long timeMills = (System.currentTimeMillis());
                    //格式化数据为A_Z的字符 + 时间
                    String element = i + "_" + simpleDateFormat.format(new Date(timeMills));
                    //设置时间戳
                    ctx.collectWithTimestamp(element, timeMills);
                    //水印时间=时间戳+0S
                    ctx.emitWatermark(new Watermark(timeMills + 0));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        dataStreamSource.print();

        //将数据转化为 Tuple2 之后对数据进行分组，
        dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String word = value.split("_")[0];
                return Tuple2.of(word, 1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).timeWindow(Time.seconds(5)).aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {


            //初始化一个累加器
            @Override
            public Tuple2<String, Integer> createAccumulator() {
                return Tuple2.of("", 0);
            }

            //累加器的计算逻辑
            @Override
            public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                return Tuple2.of(value.f0, value.f1 + accumulator.f1);
            }

            //获取累加器的结果
            @Override
            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                return accumulator;
            }

            //合并结果
            @Override
            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                return Tuple2.of(a.f0, a.f1 + b.f1);
            }
        }).print();

        environment.execute("MyWindowsAggregateFunction");

    }
}