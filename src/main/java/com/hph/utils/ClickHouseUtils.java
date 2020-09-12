package com.hph.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ClickHouseUtils {

    private static DruidDataSource dataSource;

    public static Connection getConnection() throws SQLException {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("ru.yandex.clickhouse.ClickHouseDriver");
        dataSource.setUrl("jdbc:clickhouse://hadoop102:8123/flinksink");
        dataSource.setUsername("default");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return dataSource.getConnection();
    }

    public static void main(String[] args) throws SQLException {

        Connection connection = getConnection();
    }

}
