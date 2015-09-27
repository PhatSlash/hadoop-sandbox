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
package fr.worf.hadoop.scratchpad;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class Scratch4HBaseComplete {

    /**
     * @param args the command line arguments
     * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
     */
    public static void main(String[] args) throws ZooKeeperConnectionException, IOException {
        Configuration hc;
        hc = HBaseConfiguration.create();

        /*HTableDescriptor ht = new HTableDescriptor("Test2");
         HTableDescriptor addFamily = ht.addFamily(new HColumnDescriptor("Id"));
         ht.addFamily(new HColumnDescriptor("Name"));
         System.out.println("connecting");
         HBaseAdmin hba = new HBaseAdmin(hc);
         System.out.println("Creating Table");
         hba.createTable(ht);
         System.out.println("Done......");
        
         #Creation Script
         create_namespace 'meteo'

         create 'meteo:stations', 'coordinates', 'parameters'

         put 'meteo:stations', 'test', 'coordinates:altitude', '0'
         put 'meteo:stations', 'test', 'coordinates:longitude', '0'
         put 'meteo:stations', 'test', 'coordinates:latitude', '0'


         put 'meteo:stations', 'test', 'parameters:temperature', '20.0'
         put 'meteo:stations', 'test', 'parameters:pressure', '1013.25'
         put 'meteo:stations', 'test', 'parameters:windspeed', '35.0'
         put 'meteo:stations', 'test', 'parameters:humidity', '50.0'
        
         */
        HTable table = new HTable(hc, "meteo:stations");
        Put p = new Put(Bytes.toBytes("testjava2"));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("altitude"), Bytes.toBytes(4810));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("comment"), Bytes.toBytes("Station "));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("latitude"), Bytes.toBytes(43604247));
        p.add(Bytes.toBytes("coordinates"), Bytes.toBytes("longitude"), Bytes.toBytes(1442994));

        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("temperature"), Bytes.toBytes(234));
        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("humidity"), Bytes.toBytes(452));
        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("pressure"), Bytes.toBytes(10250));
        p.add(Bytes.toBytes("parameters"), Bytes.toBytes("windspeed"), Bytes.toBytes(250));
        table.put(p);

        Get g = new Get(Bytes.toBytes("testjava2"));
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

        System.out.println("altitude : " + altitude);
        System.out.println("comment : " + comment);
        System.out.println("latitude : " + latitude);
        System.out.println("longitude : " + longitude);

        System.out.println("temperature : " + temperature);
        System.out.println("humidity : " + humidity);
        System.out.println("pressure : " + pressure);
        System.out.println("windspeed : " + windspeed);

    }
}
