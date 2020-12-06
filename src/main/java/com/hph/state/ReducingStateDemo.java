package com.hph.state;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReducingStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("B", 1L), Tuple2.of("B", 1L)).keyBy(0).flatMap(new CountWindState()).print();

        environment.execute("ReducingStateDemo");

    }

    public static class CountWindState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        ReducingState<Long> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //创建一个state 指定类型

            ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<>("sum", new ReduceFunction<Long>() {
                @Override
                public Long reduce(Long value1, Long value2) throws Exception {
                    return value1 + value2;
                }
            }, Long.class);

            //设置state的过期时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10)).cleanupFullSnapshot()
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            //指定过期时间的相关存活时间
            descriptor.enableTimeToLive(ttlConfig);
            reducingState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
            reducingState.add(value.f1);
            out.collect(Tuple2.of(value.f0, reducingState.get()));
        }
    }
}
