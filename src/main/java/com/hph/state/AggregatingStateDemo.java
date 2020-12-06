package com.hph.state;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class AggregatingStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("B", 1L), Tuple2.of("B", 1L)).keyBy(0).flatMap(new CountWindState()).print();

        environment.execute("AggregatingStateDemo");

    }

    public static class CountWindState extends RichFlatMapFunction<Tuple2<String, Long>, String> {
        private AggregatingState<String, String> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //创建一个state 指定类型
            AggregatingStateDescriptor<String, String, String> descriptor = new AggregatingStateDescriptor<>("total", new AggregateFunction<String, String, String>() {
                //初始化累加器为空的字符串
                @Override
                public String createAccumulator() {
                    return "";
                }

                //拼接字符值金和累加器

                @Override
                public String add(String value, String accumulator) {
                    return value + ">" + accumulator;
                }

                //创建累加器
                @Override
                public String getResult(String accumulator) {
                    return accumulator;
                }

                //合并结果
                @Override
                public String merge(String a, String b) {
                    return a + ">" + b;
                }
            }, String.class);
            //设置state的过期时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10)).cleanupFullSnapshot()
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            //指定过期时间的相关存活时间
            descriptor.enableTimeToLive(ttlConfig);
            aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
        }


        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<String> out) throws Exception {
            aggregatingState.add(value.f0);
            out.collect(value.f0 + ":" + aggregatingState.get());

        }
    }
}
