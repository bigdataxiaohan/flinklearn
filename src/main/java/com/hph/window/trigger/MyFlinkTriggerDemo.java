package com.hph.window.trigger;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.parser.LongParser;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Spliterator;

/**
 * 后续优化写一个Session窗口的Demo
 */
public class MyFlinkTriggerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> dataStreamSource = environment.addSource(new RichSourceFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //随机产生数据为 A-Z附带时间戳
                while (isRunning) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Thread.sleep(1000);
                    char i = (char) (65 + new Random().nextInt(25));
                    int temp = 40 + new Random().nextInt(30);
                    long timeMills = (System.currentTimeMillis());
                    //格式化数据为A_Z的字符 + 时间
                    String element = i + "_" + temp + "_" + simpleDateFormat.format(new Date(timeMills));
                    //设置时间戳
                    ctx.collectWithTimestamp(element, timeMills);
                    //水印时间=时间戳+0S
                    ctx.emitWatermark(new Watermark(timeMills + 0));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        dataStreamSource.print();

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> tupleData = dataStreamSource.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] data = value.split("_");
                return Tuple3.of(data[0], Integer.parseInt(data[1]), data[2]);
            }
        });


        SingleOutputStreamOperator<Object> process = tupleData.keyBy(new KeySelector<Tuple3<String, Integer, String>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, String> value) throws Exception {
                return value.f0;
            }
        }).timeWindow(Time.seconds(10)).trigger(new CustomProcessingTimeTrigger()).process(new ProcessWindowFunction<Tuple3<String, Integer, String>, Object, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple3<String, Integer, String>> elements, Collector<Object> out) throws Exception {
                for (Tuple3<String, Integer, String> element : elements) {
                    out.collect( "已经出现2次温度高于50°的设备" );

                }
            }
        });
        process.print("告警流==》");
        environment.execute("MyTrigger");
    }

    private static class CustomProcessingTimeTrigger extends Trigger<Tuple3<String, Integer, String>, TimeWindow> {
        private static int cnt = 0;

        @Override
        public TriggerResult onElement(Tuple3<String, Integer, String> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            if (element.f1 > 50) {
                cnt++;
            }
            if (cnt == 2) {
                cnt = 0;
                return TriggerResult.FIRE;

            } else {
                return TriggerResult.CONTINUE;
            }

        }


        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }


}