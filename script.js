package com.example.tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TF {
    // TF Mapper
    public static class TFMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordDocPair = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t", 2);
            String docId = parts[0];
            String content = parts[1];
            StringTokenizer itr = new StringTokenizer(content);
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken().toLowerCase();
                wordDocPair.set(word + "@" + docId);
                context.write(wordDocPair, one);
            }
        }
    }

    // TF Reducer
    public static class TFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
