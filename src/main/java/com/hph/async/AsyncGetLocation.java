package com.hph.async;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.hph.bean.UserLocation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class AsyncGetLocation extends RichAsyncFunction<String, UserLocation> {
    private static Gson gson;
    private static String address;
    private transient CloseableHttpAsyncClient httpClient;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化异步的HTTPClient
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        httpClient = HttpAsyncClients.custom()
                .setMaxConnPerRoute(20)
                .setDefaultRequestConfig(requestConfig).build();
        httpClient.start();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<UserLocation> resultFuture) throws Exception {
        gson = new Gson();
        UserLocation userLocation = gson.fromJson(input, UserLocation.class);
        String lng = userLocation.getLng();
        String lat = userLocation.getLat();

        String url = "https://restapi.amap.com/v3/geocode/regeo?key=8726dd1c181748cdd953c6bebbe14de9&location=" + lng + "," + lat;
        HttpGet httpGet = new HttpGet(url);

        Future<HttpResponse> future = httpClient.execute(httpGet, null);

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    HttpResponse response = future.get();
                    if (response.getStatusLine().getStatusCode() == 200) {
                        String result = EntityUtils.toString(response.getEntity());

                        JSONObject jsonObject = JSON.parseObject(result);
                        //获取位置信息
                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                        if (regeocode != null && !regeocode.isEmpty()) {
                            address = regeocode.getString("formatted_address");
                        }

                    }
                    return address;
                } catch (Exception e) {
                    return "没找到";
                }
            }
        }).thenAccept((String address) -> {
            resultFuture.complete(Collections.singleton(new UserLocation(userLocation.getUser(), userLocation.getTimestamp(), userLocation.getLng(), userLocation.getLat(), address)));
        });

    }
}
