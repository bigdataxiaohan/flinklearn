/**
 * @Classname BrodCastDemo
 * @Description TODO
 * @Date 2020/8/8 22:53
 * @Created by hph
 */

package com.hph.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

public class BrodCastDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> longDataStreamSource = executionEnvironment.generateSequence(0, 100);
        longDataStreamSource.broadcast();

    }
}
