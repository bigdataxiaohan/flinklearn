/**
 * @Classname FileSource
 * @Description TODO
 * @Date 2020/7/20 22:51
 * @Created by hph
 */

package com.hph.datasource;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;


public class FileSource {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //文件路径
        String filePath= "D:\\博客\\hphblog\\";
        Path path = new Path(filePath);
        TextInputFormat format = new TextInputFormat(path);
        format.setCharsetName("UTF-8");
        //TypeInformation 类型
        BasicTypeInfo typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        DataStreamSource dataStreamSource = environment.readFile(format, filePath, FileProcessingMode.PROCESS_ONCE, 1,
                (TypeInformation) typeInfo);
        //打印数据
        dataStreamSource.print();
        environment.execute("FileSource");
    }
}
