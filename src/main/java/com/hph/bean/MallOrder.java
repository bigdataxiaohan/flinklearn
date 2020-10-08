package com.hph.bean;

public class MallOrder {
    private String userId;
    private long timeStamp;
    private int itemId;
    private double moneyCost;


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getItemId() {
        return itemId;
    }

    public void setItemId(int itemId) {
        this.itemId = itemId;
    }

    public double getMoneyCost() {
        return moneyCost;
    }

    public void setMoneyCost(double moneyCost) {
        this.moneyCost = moneyCost;
    }

    public MallOrder() {
    }

    public MallOrder(String userId, long timeStamp, int itemId, double moneyCost) {
        this.userId = userId;
        this.timeStamp = timeStamp;
        this.itemId = itemId;
        this.moneyCost = moneyCost;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"userId\":\"")
                .append(userId).append('\"');
        sb.append(",\"timeStamp\":")
                .append(timeStamp);
        sb.append(",\"itemId\":")
                .append(itemId);
        sb.append(",\"moneyCost\":")
                .append(moneyCost);
        sb.append('}');
        return sb.toString();
    }
}
