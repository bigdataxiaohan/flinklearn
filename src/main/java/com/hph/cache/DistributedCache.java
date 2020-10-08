package com.hph.cache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.HashMap;
import java.util.List;


public class DistributedCache {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> data = executionEnvironment.generateSequence(1, 891);
        
        executionEnvironment.registerCachedFile("D:\\数据集\\Titanic-master\\train.csv", "train", true);

        SingleOutputStreamOperator<Tuple2<Long, String>> result = data.map(new RichMapFunction<Long, Tuple2<Long, String>>() {
            private HashMap<Long, String> cacheDataMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                File train = getRuntimeContext().getDistributedCache().getFile("train");
                List<String> lists = FileUtils.readLines(train);
                for (String list : lists) {
        /*             PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
                       1,0,3,"Braund, Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S     */

                    String[] data = list.split(",");
                    //缓存id 对应名字
                    cacheDataMap.put(Long.valueOf(data[0]), data[3] + "," + data[4]);
                }
            }

            @Override
            public Tuple2<Long, String> map(Long value) throws Exception {
                return Tuple2.of(value, cacheDataMap.get(value));
            }
        });
        
        result.print();
        executionEnvironment.execute();

    }
}