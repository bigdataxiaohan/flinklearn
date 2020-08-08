/**
 * @Classname HadoopFileSource
 * @Description TODO
 * @Date 2020/7/19 11:51
 * @Created by hph
 */

package com.hph.datasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HadoopFileSource {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取HDFS文件目录
        DataStreamSource<String> HadoopFileSource = environment.readTextFile("hdfs://hadoop102:9000/input/test","UTF-8");
        //打印数据
        HadoopFileSource.print();
        //执行任务
        environment.execute("HadoopFileSource");
    }
}
