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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class Scratch1HBase {

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
        System.out.println("Done......");*/

        HTable table = new HTable(hc, "Test");
        Put p = new Put(Bytes.toBytes("row1"));
        p.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"), Bytes.toBytes("Emp1"));
        p.add(Bytes.toBytes("Name"), Bytes.toBytes("col2"), Bytes.toBytes("Archana2"));
        table.put(p);

        Get g = new Get(Bytes.toBytes("row1"));
        Result r = table.get(g);
        byte[] value = r.getValue(Bytes.toBytes("Id"), Bytes.toBytes("col1"));
        byte[] value1 = r.getValue(Bytes.toBytes("Name"), Bytes.toBytes("col2"));
        String valueStr = Bytes.toString(value);
        String valueStr1 = Bytes.toString(value1);
        System.out.println("GET: " + "Id: " + valueStr + "Name: " + valueStr1);
    }

}
