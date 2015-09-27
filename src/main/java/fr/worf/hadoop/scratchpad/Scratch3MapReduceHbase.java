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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author Loïc Mercier des Rochettes <loic.mercier.d@gmail.com>
 */
public class Scratch3MapReduceHbase {

    public static class MyMapper extends TableMapper<Text, IntWritable> {

        public static final byte[] CF = "cf".getBytes();
        public static final byte[] ATTR1 = "attr1".getBytes();

        private final IntWritable ONE = new IntWritable(1);
        private final Text text = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String val = new String(value.getValue(CF, ATTR1));
            text.set(val);     // we can only emit Writables...
            context.write(text, ONE);
        }
    }

    public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public static final byte[] CF = "cf".getBytes();
        public static final byte[] COUNT = "count".getBytes();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(CF, COUNT, Bytes.toBytes(i));

            context.write(null, put);
        }
    }

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration hc;
        hc = HBaseConfiguration.create();
        Job job;
        job = new Job(hc, "rowcount");

        job.setJarByClass(Scratch3MapReduceHbase.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        //TableMapReduceUtil.initTableMapperJob("test", scan, MyMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableMapperJob(
                "test", // input table
                scan, // Scan instance to control CF and attribute selection
                MyMapper.class, // mapper class
                Text.class, // mapper output key
                IntWritable.class, // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                "test2", // output table
                MyTableReducer.class, // reducer class
                job);
        job.setNumReduceTasks(1);

        //TableMapReduceUtil.initTableMapperJob("", scan, MyMapper.class, Text.class, job);
        /*job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);

         job.setMapperClass(Scratch2MapReduce.Map.class);
         job.setCombinerClass(Scratch2MapReduce.Reduce.class);
         job.setReducerClass(Scratch2MapReduce.Reduce.class);

         job.setInputFormat(TextInputFormat.class);
         job.setOutputFormat(TextOutputFormat.class);

         FileInputFormat.setInputPaths(job, new Path("/home/slash/test/testfile1.txt"));
         FileOutputFormat.setOutputPath(job, new Path("/home/slash/test/testfile2.txt"));*/
        job.waitForCompletion(true);
    }

}
