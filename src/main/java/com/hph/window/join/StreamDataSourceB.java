package com.hph.window.join;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StreamDataSourceB extends RichParallelSourceFunction<Tuple3<String, String, Long>> {


    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        //事先准备好的数据
        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "杭州", 10000005000L),
                Tuple3.of("b", "北京", 10000011500L),

        };
        int count = 0;

        while (running && count < elements.length) {
            //数据发送出去
            ctx.collect(new Tuple3<>((String) elements[count].f0, (String) elements[count].f1, (Long) elements[count].f2));
            count++;
            Thread.sleep(1000);
        }


    }

    @Override
    public void cancel() {

    }
}
