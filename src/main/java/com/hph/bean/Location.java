package com.hph.bean;

import java.util.List;

public class Location {


    /**
     * status : 1
     * regeocode : {"addressComponent":{"city":"沧州市","province":"河北省","adcode":"130929","district":"献县","towncode":"130929213000","streetNumber":{"number":[],"direction":[],"distance":[],"street":[]},"country":"中国","township":"垒头乡","businessAreas":[[]],"building":{"name":[],"type":[]},"neighborhood":{"name":[],"type":[]},"citycode":"0317"},"formatted_address":"河北省沧州市献县垒头乡天运平衡块厂"}
     * info : OK
     * infocode : 10000
     */

    private String status;
    /**
     * addressComponent : {"city":"沧州市","province":"河北省","adcode":"130929","district":"献县","towncode":"130929213000","streetNumber":{"number":[],"direction":[],"distance":[],"street":[]},"country":"中国","township":"垒头乡","businessAreas":[[]],"building":{"name":[],"type":[]},"neighborhood":{"name":[],"type":[]},"citycode":"0317"}
     * formatted_address : 河北省沧州市献县垒头乡天运平衡块厂
     */

    private RegeocodeBean regeocode;
    private String info;
    private String infocode;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public RegeocodeBean getRegeocode() {
        return regeocode;
    }

    public void setRegeocode(RegeocodeBean regeocode) {
        this.regeocode = regeocode;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getInfocode() {
        return infocode;
    }

    public void setInfocode(String infocode) {
        this.infocode = infocode;
    }

    public static class RegeocodeBean {
        /**
         * city : 沧州市
         * province : 河北省
         * adcode : 130929
         * district : 献县
         * towncode : 130929213000
         * streetNumber : {"number":[],"direction":[],"distance":[],"street":[]}
         * country : 中国
         * township : 垒头乡
         * businessAreas : [[]]
         * building : {"name":[],"type":[]}
         * neighborhood : {"name":[],"type":[]}
         * citycode : 0317
         */

        private AddressComponentBean addressComponent;
        private String formatted_address;

        public AddressComponentBean getAddressComponent() {
            return addressComponent;
        }

        public void setAddressComponent(AddressComponentBean addressComponent) {
            this.addressComponent = addressComponent;
        }

        public String getFormatted_address() {
            return formatted_address;
        }

        public void setFormatted_address(String formatted_address) {
            this.formatted_address = formatted_address;
        }

        public static class AddressComponentBean {
            private String city;
            private String province;
            private String adcode;
            private String district;
            private String towncode;
            private StreetNumberBean streetNumber;
            private String country;
            private String township;
            private BuildingBean building;
            private NeighborhoodBean neighborhood;
            private String citycode;
            private List<List<?>> businessAreas;

            public String getCity() {
                return city;
            }

            public void setCity(String city) {
                this.city = city;
            }

            public String getProvince() {
                return province;
            }

            public void setProvince(String province) {
                this.province = province;
            }

            public String getAdcode() {
                return adcode;
            }

            public void setAdcode(String adcode) {
                this.adcode = adcode;
            }

            public String getDistrict() {
                return district;
            }

            public void setDistrict(String district) {
                this.district = district;
            }

            public String getTowncode() {
                return towncode;
            }

            public void setTowncode(String towncode) {
                this.towncode = towncode;
            }

            public StreetNumberBean getStreetNumber() {
                return streetNumber;
            }

            public void setStreetNumber(StreetNumberBean streetNumber) {
                this.streetNumber = streetNumber;
            }

            public String getCountry() {
                return country;
            }

            public void setCountry(String country) {
                this.country = country;
            }

            public String getTownship() {
                return township;
            }

            public void setTownship(String township) {
                this.township = township;
            }

            public BuildingBean getBuilding() {
                return building;
            }

            public void setBuilding(BuildingBean building) {
                this.building = building;
            }

            public NeighborhoodBean getNeighborhood() {
                return neighborhood;
            }

            public void setNeighborhood(NeighborhoodBean neighborhood) {
                this.neighborhood = neighborhood;
            }

            public String getCitycode() {
                return citycode;
            }

            public void setCitycode(String citycode) {
                this.citycode = citycode;
            }

            public List<List<?>> getBusinessAreas() {
                return businessAreas;
            }

            public void setBusinessAreas(List<List<?>> businessAreas) {
                this.businessAreas = businessAreas;
            }

            public static class StreetNumberBean {
                private List<?> number;
                private List<?> direction;
                private List<?> distance;
                private List<?> street;

                public List<?> getNumber() {
                    return number;
                }

                public void setNumber(List<?> number) {
                    this.number = number;
                }

                public List<?> getDirection() {
                    return direction;
                }

                public void setDirection(List<?> direction) {
                    this.direction = direction;
                }

                public List<?> getDistance() {
                    return distance;
                }

                public void setDistance(List<?> distance) {
                    this.distance = distance;
                }

                public List<?> getStreet() {
                    return street;
                }

                public void setStreet(List<?> street) {
                    this.street = street;
                }
            }

            public static class BuildingBean {
                private List<?> name;
                private List<?> type;

                public List<?> getName() {
                    return name;
                }

                public void setName(List<?> name) {
                    this.name = name;
                }

                public List<?> getType() {
                    return type;
                }

                public void setType(List<?> type) {
                    this.type = type;
                }
            }

            public static class NeighborhoodBean {
                private List<?> name;
                private List<?> type;

                public List<?> getName() {
                    return name;
                }

                public void setName(List<?> name) {
                    this.name = name;
                }

                public List<?> getType() {
                    return type;
                }

                public void setType(List<?> type) {
                    this.type = type;
                }
            }
        }
    }
}
