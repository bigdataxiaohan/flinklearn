package com.hph.window.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StreamDataSourceA  extends RichParallelSourceFunction<Tuple3<String,String,Long>> {


    private  volatile  boolean running = true;
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        //事先准备好的数据
        Tuple3[] elements = new Tuple3[]{
          Tuple3.of("a","1",10000005000L),  //[50000 - 60000]
          Tuple3.of("a","1",10000005400L),  //[50000 - 60000]
          Tuple3.of("a","1",10000007990L),  //[70000 - 80000]
          Tuple3.of("a","1",10000011500L),  //[10000 - 12000]
          Tuple3.of("a","1",10000010000L),  //[10000 - 110000]
          Tuple3.of("a","1",10000010800L)

        };
        int count = 0;

        while (running && count< elements.length){
            //数据发送出去
            ctx.collect(new Tuple3<>((String) elements[count].f0, (String) elements[count].f1,(Long) elements[count].f2));
            count++;
            Thread.sleep(1000);
        }


    }

    @Override
    public void cancel() {

    }
}
