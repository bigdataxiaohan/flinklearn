/**
 * @Classname SocketSource
 * @Description TODO
 * @Date 2020/7/19 11:07
 * @Created by hph
 */

package com.hph.datasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSource {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);

        socketStream.print();
        env.execute("Socket DataSource");

    }
}
