package com.hph.time;


import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Random;

public class MyWaterMarkAndTimestampsFlink11 {
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
                    String element = i + "_" + timeMills;
                    ctx.collect(element);

                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        SingleOutputStreamOperator<String> map = dataStreamSource.map(new MapFunction<String, String>() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

            @Override
            public String map(String value) throws Exception {
                String[] data = value.split("_");
                String time = simpleDateFormat.format(new Date(Long.valueOf(data[1])));
                return data[0] + "_" + time;
            }
        });

        //打印处理后的数据方便查看
        map.print();


        //提取数据中的时间戳和设置水位线
        SingleOutputStreamOperator<String> watermarks = dataStreamSource.assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {

            @Override
            public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<String>() {
                    @Override
                    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
                        String[] data = event.split("_");
                        String word = data[0];
                        String timeStamp = data[1];
                        //设置为提前1s中触发
                        output.emitWatermark(new Watermark(Long.valueOf(timeStamp) - 1000));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {

                    }
                };

            }

            //提取时间戳
            @Override
            public TimestampAssigner<String> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new TimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] data = element.split("_");
                        Long timeStamp = Long.valueOf(data[1]);
                        return timeStamp;
                    }
                };
            }
        });


        //分组  窗口每5s生成一次
        watermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] data = value.split("_");
                return Tuple2.of(data[0], 1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).timeWindow(Time.seconds(5)).sum(1).print();
        environment.execute("MyWaterMarkAndTimestampsFlink11");
    }


}