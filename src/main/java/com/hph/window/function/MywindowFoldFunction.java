package com.hph.window.function;

import org.apache.flink.api.common.functions.FoldFunction;
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

public class MywindowFoldFunction {
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

        //将数据转化为 Tuple2 之后对数据进行分组，同是创建一个窗口 每5秒滑动一次
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
        }).window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(2))).fold("hphblog->", new FoldFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String fold(String accumulator, Tuple2<String, Integer> value) throws Exception {
                return accumulator + value.f0;
            }
        }).print();
        environment.execute("MywindowFoldFunction");
        // fold() cannot be used with session windows or other mergeable windows.
    }
}
