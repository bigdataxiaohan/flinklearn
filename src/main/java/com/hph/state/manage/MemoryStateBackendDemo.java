package com.hph.state.manage;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class MemoryStateBackendDemo {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://hadoop102:9000/flink/checkpoints", true);
        executionEnvironment.setStateBackend((StateBackend) rocksDBStateBackend);


    }
}
