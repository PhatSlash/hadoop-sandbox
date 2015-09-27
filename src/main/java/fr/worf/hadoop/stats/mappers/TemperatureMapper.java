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
package fr.worf.hadoop.stats.mappers;

import java.io.IOException;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class TemperatureMapper extends TableMapper<Text, IntWritable> {

    public static final byte[] CF = "parameters".getBytes();
    public static final byte[] ATTR1 = "temperature".getBytes();

    private final IntWritable valueout = new IntWritable(1);
    private final Text text = new Text();
    
    private static Logger logger = Logger.getLogger(TemperatureMapper.class);

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        logger.debug(CF);
        logger.debug(ATTR1);
        logger.debug(value.toString());
        logger.debug(value.getValue(CF, ATTR1));
        int val = Bytes.toInt(value.getValue(CF, ATTR1));
        text.set("Temperature moyenne");
        valueout.set(val);
        context.write(text, valueout);
    }
}
