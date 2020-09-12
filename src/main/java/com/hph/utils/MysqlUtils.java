package com.hph.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class MysqlUtils {
    private static DruidDataSource dataSource;

    public static Connection getConnection() throws SQLException {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://hadoop102:3306/flinksink");
        dataSource.setUsername("root");
        dataSource.setPassword("000000");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return dataSource.getConnection();
    }

}
