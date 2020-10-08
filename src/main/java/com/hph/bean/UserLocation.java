package com.hph.bean;

public class UserLocation {
    private String user;
    private long timestamp;
    private String lng;
    private String lat;
    private String address;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public UserLocation(String user, long timestamp, String lng, String lat, String address) {
        this.user = user;
        this.timestamp = timestamp;
        this.lng = lng;
        this.lat = lat;
        this.address = address;
    }


    public UserLocation() {
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLng() {
        return lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    @Override
    public String toString() {
        return "UserLocation{" +
                "user='" + user + '\'' +
                ", timestamp=" + timestamp +
                ", lng='" + lng + '\'' +
                ", lat='" + lat + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
