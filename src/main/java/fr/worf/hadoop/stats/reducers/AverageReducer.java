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
package fr.worf.hadoop.stats.reducers;

import fr.worf.hadoop.stats.mappers.TemperatureMapper;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class AverageReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    public static final byte[] CF = "results".getBytes();
    public static final byte[] COL = "average temperature".getBytes();
    
    private static Logger logger = Logger.getLogger(AverageReducer.class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        int count = 0;
        for (IntWritable val : values) {
           result += val.get();
           ++count;
        }
        result /= count;
        
        logger.debug("Average : " + result);
        logger.debug("Nombre : " + count);
        
        Put put = new Put(Bytes.toBytes(key.toString()));
        put.add(CF, COL, Bytes.toBytes(result));

        context.write(null, put);
    }
}
