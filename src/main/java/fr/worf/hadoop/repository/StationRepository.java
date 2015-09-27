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
package fr.worf.hadoop.repository;

import fr.worf.hadoop.repository.dataobjects.Station;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class StationRepository {

    private final Configuration hc;
    private final HTable table;

    public StationRepository() throws IOException {
        hc = HBaseConfiguration.create();
        table = new HTable(hc, "meteo:stations");
    }

    public void put(Station s) throws IOException {
        Put p = new Put(Bytes.toBytes(s.getRowName()));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("altitude"), Bytes.toBytes(s.getAltitude()));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("comment"), Bytes.toBytes(s.getComment()));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("latitude"), Bytes.toBytes(s.getLatitude()));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("longitude"), Bytes.toBytes(s.getLongitude()));

        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("temperature"), Bytes.toBytes(s.getTemperature()));
        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("humidity"), Bytes.toBytes(s.getHumidity()));
        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("pressure"), Bytes.toBytes(s.getPressure()));
        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("windspeed"), Bytes.toBytes(s.getWindspeed()));
        table.put(p);
    }

    public Station get(String row) throws IOException {
        Get g = new Get(Bytes.toBytes(row));
        Result r = table.get(g);
        byte[] altitudeb = r.getValue(Bytes.toBytes("coordinates"), Bytes.toBytes("altitude"));
        byte[] commentb = r.getValue(Bytes.toBytes("coordinates"), Bytes.toBytes("comment"));
        byte[] latitudeb = r.getValue(Bytes.toBytes("coordinates"), Bytes.toBytes("latitude"));
        byte[] longitudeb = r.getValue(Bytes.toBytes("coordinates"), Bytes.toBytes("longitude"));

        byte[] temperatureb = r.getValue(Bytes.toBytes("parameters"), Bytes.toBytes("temperature"));
        byte[] humidityb = r.getValue(Bytes.toBytes("parameters"), Bytes.toBytes("humidity"));
        byte[] pressureb = r.getValue(Bytes.toBytes("parameters"), Bytes.toBytes("pressure"));
        byte[] windspeedb = r.getValue(Bytes.toBytes("parameters"), Bytes.toBytes("windspeed"));

        int altitude = Bytes.toInt(altitudeb);
        String comment = Bytes.toString(commentb);
        int latitude = Bytes.toInt(latitudeb);
        int longitude = Bytes.toInt(longitudeb);

        int temperature = Bytes.toInt(temperatureb);
        int humidity = Bytes.toInt(humidityb);
        int pressure = Bytes.toInt(pressureb);
        int windspeed = Bytes.toInt(windspeedb);

        Station s = new Station();
        s.setAltitude(altitude);
        s.setComment(comment);
        s.setLatitude(latitude);
        s.setLongitude(longitude);

        s.setTemperature(temperature);
        s.setHumidity(humidity);
        s.setPressure(pressure);
        s.setWindspeed(windspeed);
        return s;
    }
}
