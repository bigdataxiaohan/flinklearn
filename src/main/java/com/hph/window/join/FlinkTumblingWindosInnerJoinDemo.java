package com.hph.window.join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkTumblingWindosInnerJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //通过Env设置并行度为1，即以后所有的DataStream的并行度都是1
        env.setParallelism(1);

        //设置数据源
        //第一个流
        DataStreamSource<Tuple3<String, String, Long>> leftSource = env.addSource(new StreamDataSourceA());
        DataStreamSource<Tuple3<String, String, Long>> rightSource = env.addSource(new StreamDataSourceB());

        //提取时间
        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStream =
                leftSource.assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Tuple3<String, String, Long>>() {
                            @Override
                            public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple3<String, String, Long>>() {
                                    @Override
                                    public void onEvent(Tuple3<String, String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        output.emitWatermark(new Watermark(event.f2 - 1000));

                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {

                                    }
                                };

                            }

                            @Override
                            public TimestampAssigner<Tuple3<String, String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new TimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                };
                            }
                        }
                );


        SingleOutputStreamOperator<Tuple3<String, String, Long>> rightStream = rightSource.assignTimestampsAndWatermarks(
                new WatermarkStrategy<Tuple3<String, String, Long>>() {
                    @Override
                    public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Tuple3<String, String, Long>>() {
                            @Override
                            public void onEvent(Tuple3<String, String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                output.emitWatermark(new Watermark(event.f2 - 1000));

                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {

                            }
                        };

                    }

                    @Override
                    public TimestampAssigner<Tuple3<String, String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new TimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        };
                    }


                }

        );


        //  join操作
        leftStream.join(rightStream)
                .where(new InnerSelectKey())
                .equalTo(new InnerSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Object>() {
                    @Override
                    public Object join(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right) throws Exception {
                        return new Tuple5<>(left.f0, left.f1, right.f1, left.f2, right.f2);
                    }
                }).print();

        env.execute("Stream Join");


    }

    public static class InnerSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> value) throws Exception {
            return value.f0;
        }

    }
}
