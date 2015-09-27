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

import fr.worf.hadoop.stats.mappers.TemperatureMapper;
import fr.worf.hadoop.stats.mappers.WeatherIndexMapper;
import fr.worf.hadoop.stats.reducers.GroupReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class GroupWeatherIndexJob extends Configured implements Tool {
        private static final Configuration hc = HBaseConfiguration.create();
    private final Job job;

    public GroupWeatherIndexJob() throws IOException {
        job = new Job(hc, "average");
        job.setJarByClass(AverageTemperatureJob.class);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob("meteo:stations", scan, WeatherIndexMapper.class, Text.class, IntWritable.class, job);

        TableMapReduceUtil.initTableReducerJob("meteo:results", GroupReducer.class, job);

        job.setNumReduceTasks(1);
        System.out.print("Lancement du job");
        if (job.waitForCompletion(true)) {
            System.out.println("Ok");
        } else {
            System.out.println("Ko");
        }
        return 0;
    }
}
