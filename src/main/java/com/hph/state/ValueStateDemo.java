package com.hph.state;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ValueStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("A", 1L), Tuple2.of("B", 1L), Tuple2.of("B", 1L)).keyBy(0).flatMap(new CountWindState()).print();

        environment.execute("ValueStateDemo");

    }

    public static class CountWindState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        //定义ValueState
        private transient ValueState<Tuple2<String, Long>> sum;


        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {

            Tuple2<String, Long> currentSum;
            // 访问ValueState
            if (sum.value() == null) {
                currentSum = Tuple2.of("NULL", 0L);
            } else {
                currentSum = sum.value();
            }
            // 更新
            currentSum.f0 += 1;
            // 第二个元素加1
            currentSum.f1 += value.f1;
            sum.update(currentSum);
            // 如果count的值大于等于3，并清空state
            if (currentSum.f1 >= 1) {
                out.collect(new Tuple2<>(value.f0, currentSum.f1));
            }


        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //创建一个state 指定类型
            ValueStateDescriptor<Tuple2<String, Long>> descriptor = new ValueStateDescriptor<>("mycount", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
            }));
            //设置state的过期时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10)).cleanupFullSnapshot()
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            //指定过期时间的相关存活时间
            descriptor.enableTimeToLive(ttlConfig);
            sum = getRuntimeContext().getState(descriptor);
        }
    }
}
