/**
 * @Classname RichMapFunctionDemo
 * @Description TODO
 * @Date 2020/8/9 16:43
 * @Created by hph
 */

package com.hph.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichMapFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);

        //传入功能更加强大的RichMapFunction
        SingleOutputStreamOperator<Integer> map = nums.map(new RichMapFunction<Integer, Integer>() {
            //open,在构造方法之后，map 方法之前，执行一次，并且可以拿到全局的配置
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });
        //sink
        map.print();
        env.execute("RichMapFunction Job");
    }
}
