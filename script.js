package com.example.tfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DF {
    // DF Mapper
    public static class DFMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text docId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("@");
            word.set(parts[0]);
            docId.set(parts[1]);
            context.write(word, docId);
        }
    }

    // DF Reducer
    public static class DFReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueDocs = new HashSet<>();
            for (Text val : values) {
                uniqueDocs.add(val.toString());
            }
            result.set(uniqueDocs.size());
            context.write(key, result);
        }
    }
}
