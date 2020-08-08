/**
 * @Classname MySource
 * @Description TODO
 * @Date 2020/7/19 17:46
 * @Created by hph
 */

package com.hph.datasource;

import com.hph.datasource.producer.MyRedisSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

public class MySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<HashMap<String, String>> hashMapDataStreamSource = environment.addSource(new MyRedisSource());
        hashMapDataStreamSource.print();

        environment.execute("MyRedis Source");
    }
}
