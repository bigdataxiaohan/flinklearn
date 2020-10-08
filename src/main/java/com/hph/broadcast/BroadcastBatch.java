package com.hph.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadcastBatch {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Jedis jedis = new Jedis("hadoop102", 6379);
        Map<String, String> redisResultMap = jedis.hgetAll("broadcast");
        ArrayList<Map<String, String>> broadCastBD = new ArrayList<>();

        broadCastBD.add(redisResultMap);

        DataSet<Map<String, String>> bdDataSet = env.fromCollection(broadCastBD);

        //源数据  需要与广播变量关联。
        MapOperator<Long, String> data = env.generateSequence(1, 891).map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return String.valueOf(value);
            }
        });

        MapOperator<String, Tuple2<String, String>> result = data.map(new RichMapFunction<String, Tuple2<String, String>>() {
            List<HashMap<String, String>> broadCastMap = new ArrayList<HashMap<String, String>>();
            HashMap<String, String> allMap = new HashMap<String, String>();


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return Tuple2.of(String.valueOf(value), allMap.get(value));
            }
        }).withBroadcastSet(bdDataSet, "broadCastMapName");
        result.print();


    }
}
