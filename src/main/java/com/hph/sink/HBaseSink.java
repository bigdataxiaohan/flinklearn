package com.hph.sink;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HBaseSink {
    public static org.apache.hadoop.conf.Configuration conf;
    private static Connection connection;
    private static Table table;
    private static final Logger logger = LoggerFactory.getLogger(HBaseSink.class);


    public static void main(String[] args) throws Exception {

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


        //添加kafka数据源
        DataStreamSource<String> order = environment.addSource(new FlinkKafkaConsumer<>(
                "Order_Reduce",
                new SimpleStringSchema(),
                props
        ));

        order.addSink(new RichSinkFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                connection = ConnectionFactory.createConnection(conf);
                table = connection.getTable(TableName.valueOf("flink-hbase"));
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {

                super.close();
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                Gson gson = new Gson();
                OrderBean orderBean = gson.fromJson(value, OrderBean.class);
                Put put = new Put(Bytes.toBytes(orderBean.getUserId()));
                put.addColumn(Bytes.toBytes("province"), Bytes.toBytes(String.valueOf(orderBean.getProvinceCode())), Bytes.toBytes(String.valueOf(orderBean.getMoney())));
                table.put(put);
                table.close();
            }
        });


        environment.execute("Flink HBase Sink");

    }


}
