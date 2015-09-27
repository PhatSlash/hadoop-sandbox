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
package fr.worf.hadoop;

import fr.worf.hadoop.stats.jobs.AverageTemperatureJob;
import fr.worf.hadoop.stats.jobs.GroupTemperatureJob;
import fr.worf.hadoop.stats.jobs.GroupWeatherIndexJob;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class Main {

    private static Logger logger = Logger.getLogger(Main.class);

    public static void showResults() throws IOException {
        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "meteo:results");

        Get getavg = new Get("Temperature moyenne".getBytes());
        Get getgroups = new Get("Temperature ressentie".getBytes());

        Result avg = table.get(getavg);
        Result groups = table.get(getgroups);

        logger.debug(Bytes.toInt(avg.getValue("results".getBytes(), "average temperature".getBytes())));
        logger.debug(Bytes.toInt(avg.getValue("results".getBytes(), "average temperature".getBytes())));
        logger.debug(Bytes.toInt(avg.getValue("results".getBytes(), "Average".getBytes())));
        logger.debug(Bytes.toInt(avg.getValue("results".getBytes(), "Cold".getBytes())));
        logger.debug(Bytes.toInt(avg.getValue("results".getBytes(), "Hot".getBytes())));
        logger.debug(Bytes.toInt(groups.getValue("results".getBytes(), "Average".getBytes())));
        logger.debug(Bytes.toInt(groups.getValue("results".getBytes(), "Cold".getBytes())));
        logger.debug(Bytes.toInt(groups.getValue("results".getBytes(), "Hot".getBytes())));
    }

    public static void main(String[] args) throws Exception {
        AverageTemperatureJob avg = new AverageTemperatureJob();
        avg.run(null);
        //GroupTemperatureJob grp = new GroupTemperatureJob();
        GroupWeatherIndexJob wigrp = new GroupWeatherIndexJob();
        wigrp.run(null);
        showResults();
    }
}
