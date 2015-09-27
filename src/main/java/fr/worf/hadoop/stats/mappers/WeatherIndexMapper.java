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
public class WeatherIndexMapper extends TableMapper<Text, IntWritable> {

    public static final byte[] CF = "parameters".getBytes();
    public static final byte[] TEMP_ATTR = "temperature".getBytes();
    public static final byte[] PRES_ATTR = "pressure".getBytes();
    public static final byte[] HUMI_ATTR = "humidity".getBytes();
    public static final byte[] WIND_ATTR = "windspeed".getBytes();

    private final IntWritable valueout = new IntWritable(1);
    private final Text text = new Text();

    private static Logger logger = Logger.getLogger(TemperatureMapper.class);

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        logger.debug(CF);
        logger.debug(TEMP_ATTR);
        logger.debug(value.toString());
        logger.debug(value.getValue(CF, TEMP_ATTR));
        // Division par 10 des attributs car ils sont *10 en base pour n'avoir que des int et garder une percision au 1/10.
        double temperature = Bytes.toInt(value.getValue(CF, TEMP_ATTR)) / 10;
        double windspeed = Bytes.toInt(value.getValue(CF, WIND_ATTR)) / 10;
        double humidity = Bytes.toInt(value.getValue(CF, HUMI_ATTR)) / 10;

        double index = 0;
        if (temperature < 10) {
            // Calcul de la temperture ressentie par refroidissement éolien
            index = 13.12 + 0.6215 * temperature + (0.3965 * temperature - 11.37) * windspeed;
        } else {
            // calcul de la racine huitème du taux d'humidité
            double racine = Math.exp(1.0/8.0* Math.log(humidity));
            
            // calcul du point de rosée
            double pr = racine * (112+(0.9*temperature)) + (0.1*temperature) - 112;
            
            // Calcul de l'indice humidex
            index = temperature + 0.5555 * (6.11 * Math.exp(5417.7530 * (1 / 273.16 - 1 / pr)) - 10);
        }

        text.set("Temperature ressentie");
        valueout.set((int) Math.round(index * 10));
        context.write(text, valueout);
    }
}
