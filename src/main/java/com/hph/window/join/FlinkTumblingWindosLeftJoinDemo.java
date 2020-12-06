package com.hph.window.join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class FlinkTumblingWindosLeftJoinDemo {
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
        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStream = leftSource.assignTimestampsAndWatermarks(
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


        // left  join操作

        leftStream.coGroup(rightStream)
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .apply(new LeftJoin())
                .print();

        env.execute("Stream  Left Join");

    }

    public static class LeftJoin implements CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {


        @Override
        public void coGroup(Iterable<Tuple3<String, String, Long>> leftElements, Iterable<Tuple3<String, String, Long>> rightElements, Collector<Tuple5<String, String, String, Long, Long>> collector) throws Exception {
            for (Tuple3<String, String, Long> leftElement : leftElements) {
                boolean hadElements = false;
                //如果左边的流join上了右边的流rightElements就不为空
                for (Tuple3<String, String, Long> rightElement : rightElements) {
                    //将join上的数据输出
                    collector.collect(new Tuple5<String, String, String, Long, Long>(leftElement.f0, leftElement.f1, rightElement.f1, rightElement.f2, leftElement.f2));
                    hadElements = true;
                }
                if (!hadElements) {
                    //没有
                    collector.collect(new Tuple5<>(leftElement.f0, leftElement.f1, "null", leftElement.f2, -1L));
                }
            }
        }
    }


    public static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> value) throws Exception {
            return value.f0;
        }
    }

    public static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> value) throws Exception {
            return value.f0;
        }
    }


}
