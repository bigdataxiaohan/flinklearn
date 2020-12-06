package com.hph.state;


import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.*;


public class ListStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("B", 1L), Tuple2.of("B", 1L)).keyBy(0).flatMap(new CountWindState()).print();

        environment.execute("ListStateDemo");

    }

    public static class CountWindState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private ListState<Tuple2<String, Long>> listState;
        long sum = 0L;


        @Override

        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {

            if (null == listState.get()) {
                listState.addAll(Collections.emptyList());
            }
            listState.add(value);
            ArrayList<Tuple2<String, Long>> allElement = Lists.newArrayList(listState.get());

            for (Tuple2<String, Long> tuple2 : allElement) {
                if (tuple2.f0 == value.f0) {
                    sum += value.f1;
                    out.collect(Tuple2.of(value.f0, sum));
                }
            }
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //创建一个state 指定类型
            ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<Tuple2<String, Long>>("myListState", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
            }));
            //设置state的过期时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10)).cleanupFullSnapshot()
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            //指定过期时间的相关存活时间
            descriptor.enableTimeToLive(ttlConfig);
            listState = getRuntimeContext().getListState(descriptor);

        }
    }
}
