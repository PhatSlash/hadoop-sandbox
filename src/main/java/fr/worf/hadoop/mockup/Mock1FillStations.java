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
package fr.worf.hadoop.mockup;

import fr.worf.hadoop.repository.StationRepository;
import fr.worf.hadoop.repository.dataobjects.Station;
import java.io.IOException;
import java.util.Random;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class Mock1FillStations {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        StationRepository sr = new StationRepository();
        Station s = new Station();
        Random r = new Random();

        for (int i = 0; i < 10000; ++i) {
            s.setRowName("mock-" + i);
            
            s.setAltitude(r.nextInt(3000));
            s.setComment("Auto-Random");
            s.setLatitude(r.nextInt(20000000));
            s.setLongitude(r.nextInt(30000000));
            
            s.setHumidity(r.nextInt(1000));
            s.setTemperature(r.nextInt(500));
            s.setWindspeed(r.nextInt(1500));
            s.setPressure(9500 + r.nextInt(1000));
            
            sr.put(s);
        }

    }

}
