/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.contapalavra;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ContaPalavraReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
 
    @Override
    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int limit = Integer.parseInt(conf.get("limit"));

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        if (sum >= limit) {
            context.write(text, new IntWritable(sum));
        }
    }
}