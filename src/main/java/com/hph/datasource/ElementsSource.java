/**
 * @Classname ElementsSource
 * @Description TODO
 * @Date 2020/7/19 11:24
 * @Created by hph
 */

package com.hph.datasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class ElementsSource {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //根据元素可以生成流数据
        DataStreamSource<String> elements = environment.fromElements("flink", "bigdata", "spark");
        //从1-100 生成数据流
        DataStreamSource<Long> generateSequence = environment.generateSequence(1, 100);
        //从数组中生成数据
        DataStreamSource<String> collectionDataSource = environment.fromCollection(Arrays.asList("Collection_1",
                "Collection_2", "Collection_3"));

        collectionDataSource.print();
        generateSequence.print();
        elements.print();

        environment.execute("ElementsSource");
    }
}
