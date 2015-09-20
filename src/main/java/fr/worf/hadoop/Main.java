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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //int rc = ToolRunner.run(new TopK(), args);
        //System.exit(rc);
        System.out.println("Test Hadoop First Milestone");
    }

    @Override
    public int run(String[] strings) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
