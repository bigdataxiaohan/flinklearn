package com.hph.sink;

import com.google.gson.Gson;
import com.hph.bean.OrderBean;
import com.hph.utils.MysqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class MysqlSqlSink extends RichSinkFunction<String> {
    private PreparedStatement ps;
    private Connection connection;
    private int i;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取数据库连接
        connection = MysqlUtils.getConnection();
        String sql = "insert into flinksink.order (user_id,province_code,city_code,money) values (?,?,?,?); ";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭并释放资源
        if (connection != null) {
            connection.close();
        }

        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        Gson gson = new Gson();
        if (i <= 100) {
            OrderBean orderBean = gson.fromJson(value, OrderBean.class);
            ps.setString(1, orderBean.getUserId());
            ps.setInt(2, orderBean.getProvinceCode());
            ps.setInt(3, orderBean.getCityCode());
            ps.setDouble(4, orderBean.getMoney());
            ps.addBatch();
            i++;
        }
        if (i >= 100) {
            ps.executeBatch();
            i = 0;
        }
    }


}
