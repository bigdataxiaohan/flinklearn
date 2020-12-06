package com.hph.state.manage;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RocksdbStateAndToleration {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpointing 每隔50分钟
        env.enableCheckpointing(1000 * 50);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(2)));
        //默认的策略是固定时间内无限重启
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 8888);

        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(args[0], false);

        //从界面获取参数
        env.setStateBackend((StateBackend) rocksDBStateBackend);
        //程序异常或者认为Cannel掉不删除checkpoting 里面的数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {


                if (s.startsWith("bug")) {
                    throw new RuntimeException("程序出现BUG");
                }
                return Tuple2.of(s, 1);
            }
        });
        wordAndOne.keyBy(s -> s.f0).sum(1).print();

        env.execute("RestartStrateegiesDemo");

    }
}
