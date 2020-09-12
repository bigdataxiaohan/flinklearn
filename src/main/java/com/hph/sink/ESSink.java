/**
 * @Classname ElasticSearchSink
 * @Description TODO
 * @Date 2020/8/9 18:38
 * @Created by hph
 */

package com.hph.sink;


import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class ESSink {
    public static void main(String[] args) throws Exception {
        //初始化环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        //指定kafka的Broker地址
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //设置组ID
        props.setProperty("group.id", "flink");
        props.setProperty("auto.offset.reset", "earliest");
        //kafka自动提交偏移量，
        props.setProperty("enable.auto.commit", "false");
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        DataStreamSource<String> order = environment.addSource(new FlinkKafkaConsumer<>(
                "Order_Reduce",
                new SimpleStringSchema(),
                props
        ));


        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200, "http"));


        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {

                    public IndexRequest createIndexRequest(String element) {
                        Gson gson = new Gson();
                        OrderBean orderBean = gson.fromJson(element, OrderBean.class);
                        Map<String, String> json = new HashMap<>();


                        json.put("province_code", String.valueOf(orderBean.getProvinceCode()));
                        json.put("city_code", String.valueOf(orderBean.getCityCode()));
                        json.put("user_id", String.valueOf(orderBean.getUserId()));
                        json.put("money", String.valueOf(orderBean.getMoney()));

                        return Requests.indexRequest()
                                .index("order_from_kafka")
                                .type("my-type")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushBackoff(true);
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
        esSinkBuilder.setBulkFlushBackoffDelay(1000L);
        esSinkBuilder.setBulkFlushMaxActions(100);
        esSinkBuilder.setBulkFlushBackoffRetries(8);
        esSinkBuilder.setBulkFlushMaxSizeMb(10);
        esSinkBuilder.setBulkFlushInterval(100);
        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler() {
            @Override
            public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
                if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                    // full queue; re-add document for indexing
                    indexer.add(action);
                } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                    // malformed document; simply drop request without failing sink

                } else {
                    // for all other failures, fail the sink
                    // here the failure is simply rethrown, but users can also choose to throw custom exceptions
                    throw failure;
                }
            }
        });


        order.addSink(esSinkBuilder.build());
        environment.execute("elasticsearch test");
    }
}