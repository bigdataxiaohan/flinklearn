/**
 * @Classname MyRediSource
 * @Description TODO
 * @Date 2020/7/19 17:27
 * @Created by hph
 */

package com.hph.datasource.producer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

public class MyRedisSource extends RichSourceFunction<HashMap<String, String>> {

    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);
    private Jedis jedis = null;
    private  boolean isRuning =true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.jedis = new Jedis("hadoop102", 6379);
    }

    @Override
    public void run(SourceFunction.SourceContext<HashMap<String, String>> ctx) throws Exception {
        HashMap<String, String> KVMap = new HashMap<>();

        while (isRuning) {
            try {
                Map<String, String> mysource = jedis.hgetAll("mysource");
                KVMap.clear();
                for (Map.Entry<String, String> entry : mysource.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    System.out.println("key:" + key + "," + "value:" + value);
                    for (String split : splits) {
                        // k   v
                        KVMap.put(key, value);
                    }
                }
                if (KVMap.size() > 0) {
                    ctx.collect(KVMap);
                } else {
                    logger.warn("从redis中获取的数据为空");
                }
                Thread.sleep(10000);
            } catch (JedisConnectionException e) {
                logger.warn("redis连接异常，需要重新连接", e.getCause());
                jedis = new Jedis("hadoop102", 6379);
            } catch (Exception e) {
                logger.warn(" source 数据源异常", e.getCause());
            }
            Thread.sleep(10000);
        }
    }

    @Override
    public void cancel() {
        try {
            isRuning=false;
        }catch (Exception e){
            logger.warn("redis连接异常，需要重新连接", e.getCause());
            jedis.close();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        isRuning=false;
        while (isRuning) {
            jedis.close();
        }
    }
}


