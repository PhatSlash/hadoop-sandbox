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
package fr.worf.hadoop.stats.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class AverageTemperatureJobTest {

    public AverageTemperatureJobTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of run method, of class AverageTemperatureJob.
     * @throws java.lang.Exception
     */
    @Test
    public void testRun() throws Exception {
        Configuration hc;
        hc = HBaseConfiguration.create();

        AverageTemperatureJob instance = new AverageTemperatureJob();
        instance.run(null);

        HTable table = new HTable(hc, "meteo:results");
        Get g = new Get(Bytes.toBytes("Temperature moyenne"));
        Result r = table.get(g);
        
        byte[] value = r.getValue(Bytes.toBytes("results"), Bytes.toBytes("temperature moyenne"));
        assertTrue("Valeur présente", value != null);
    }

}
