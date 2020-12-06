package com.hph.window.trigger;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * 后续优化写一个Session窗口的Demo
 */
public class MyTrigger {

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
                    long timeMills = (System.currentTimeMillis());
                    //格式化数据为A_Z的字符 + 时间
                    String element = i + "_" + simpleDateFormat.format(new Date(timeMills));
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


        //将数据转化为 Tuple2 之后对数据进行分组，同是创建一个窗口 每5秒滑动一次
        dataStreamSource.map(new MapFunction<String, org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {
            @Override
            public org.apache.flink.api.java.tuple.Tuple2<String, Integer> map(String value) throws Exception {
                String word = value.split("_")[0];
                return org.apache.flink.api.java.tuple.Tuple2.of(word, 1);
            }
        }).keyBy(new KeySelector<org.apache.flink.api.java.tuple.Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).timeWindow(Time.seconds(3)).trigger(new CustomProcessingTimeTrigger()).sum(1).print();

        environment.execute("MyTrigger");
    }


    private static class CustomProcessingTimeTrigger extends Trigger<Tuple2<String, Integer>, TimeWindow> {
        int flag = 0;

        private CustomProcessingTimeTrigger() {
        }

        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            if (flag > 2) {
                flag = 0;
                return TriggerResult.FIRE;
            } else {
                flag++;
            }
            return TriggerResult.CONTINUE;
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
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window,
                            OnMergeContext ctx) {
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                ctx.registerProcessingTimeTimer(windowMaxTimestamp);
            }
        }
    }
}