/*
 * Copyright 2015 Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.worf.hadoop.repository.dataobjects;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class Station {
    private String rowName;
    private int altitude;
    private String comment;
    private int latitude;
    private int longitude;

    private int temperature;
    private int humidity;
    private int pressure;
    private int windspeed;

    /**
     * @return the altitude
     */
    public int getAltitude() {
        return altitude;
    }

    /**
     * @param altitude the altitude to set
     */
    public void setAltitude(int altitude) {
        this.altitude = altitude;
    }

    /**
     * @return the comment
     */
    public String getComment() {
        return comment;
    }

    /**
     * @param comment the comment to set
     */
    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * @return the latitude
     */
    public int getLatitude() {
        return latitude;
    }

    /**
     * @param latitude the latitude to set
     */
    public void setLatitude(int latitude) {
        this.latitude = latitude;
    }

    /**
     * @return the longitude
     */
    public int getLongitude() {
        return longitude;
    }

    /**
     * @param longitude the longitude to set
     */
    public void setLongitude(int longitude) {
        this.longitude = longitude;
    }

    /**
     * @return the temperature
     */
    public int getTemperature() {
        return temperature;
    }

    /**
     * @param temperature the temperature to set
     */
    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    /**
     * @return the humidity
     */
    public int getHumidity() {
        return humidity;
    }

    /**
     * @param humidity the humidity to set
     */
    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    /**
     * @return the pressure
     */
    public int getPressure() {
        return pressure;
    }

    /**
     * @param pressure the pressure to set
     */
    public void setPressure(int pressure) {
        this.pressure = pressure;
    }

    /**
     * @return the windspeed
     */
    public int getWindspeed() {
        return windspeed;
    }

    /**
     * @param windspeed the windspeed to set
     */
    public void setWindspeed(int windspeed) {
        this.windspeed = windspeed;
    }

    public void getComment(String autogénéré) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * @return the rowName
     */
    public String getRowName() {
        return rowName;
    }

    /**
     * @param rowName the rowName to set
     */
    public void setRowName(String rowName) {
        this.rowName = rowName;
    }
}
