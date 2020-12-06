package com.hph.state;


import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class MapStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("B", 1L), Tuple2.of("B", 1L)).keyBy(0).flatMap(new CountWindState()).print();

        environment.execute("MapStateDemo");

    }

    public static class CountWindState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        //定义ValueState
        private transient MapState<String, Long> mapState;
        //定义只
        private Long cnt = 0L;

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
            //放入MapState
            mapState.put(value.f0, value.f1);
            //取出值
            Iterable<String> iterable = mapState.keys();
            //取出所有的key做计数
            ArrayList<String> arrayList = Lists.newArrayList(iterable);

            for (String word : arrayList) {
                cnt++;
                out.collect(Tuple2.of(word, cnt));
            }


        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //创建一个state 指定类型
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("average", String.class, Long.class);
            //设置state的过期时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10)).cleanupFullSnapshot()
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            //指定过期时间的相关存活时间
            descriptor.enableTimeToLive(ttlConfig);
            mapState = getRuntimeContext().getMapState(descriptor);
        }
    }
}
