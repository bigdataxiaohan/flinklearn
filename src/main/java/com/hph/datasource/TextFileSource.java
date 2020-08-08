/**
 * @Classname FileSource
 * @Description TODO
 * @Date 2020/7/19 11:46
 * @Created by hph
 */

package com.hph.datasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TextFileSource {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取路径
        DataStreamSource<String> dataStreamSource = environment.readTextFile("D:\\博客\\hphblog\\package-lock.json");
        //输出数据
        dataStreamSource.print();
        //执行任务
        environment.execute("FileSource");

    }
}
