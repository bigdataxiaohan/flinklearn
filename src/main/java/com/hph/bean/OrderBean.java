package com.hph.bean;


import java.io.Serializable;

/**
 * @Classname OrderBean
 * @Description TODO
 * @Date 2020/7/30 15:26
 * @Created by hph
 */

public class OrderBean implements Serializable {
    private int provinceCode;
    private int cityCode;
    private String userId;
    private Double money;

    public OrderBean(int provinceCode, int cityCode, int i, Double money) {
    }

    public int getProvinceCode() {
        return provinceCode;
    }

    public void setProvinceCode(int provinceCode) {
        this.provinceCode = provinceCode;
    }

    public int getCityCode() {
        return cityCode;
    }

    public void setCityCode(int cityCode) {
        this.cityCode = cityCode;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public OrderBean() {
    }

    public OrderBean(int provinceCode, int cityCode, String userId, Double money) {
        this.provinceCode = provinceCode;
        this.cityCode = cityCode;
        this.userId = userId;
        this.money = money;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "provinceCode=" + provinceCode +
                ", cityCode=" + cityCode +
                ", userId='" + userId + '\'' +
                ", money=" + money +
                '}';
    }
}

