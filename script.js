package com.example.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDF {
    // TF-IDF Mapper
    public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
        private Text wordDocPair = new Text();
        private Text tfAndDf = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t");
            String[] wordDoc = parts[0].split("@");
            String word = wordDoc[0];
            String docId = wordDoc[1];
            String tf = parts[1];
            context.write(new Text(word), new Text(docId + "=" + tf));
        }
    }

    // TF-IDF Reducer
    public static class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable tfidf = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalDocs = 10; // Example: total number of documents
            int docCount = 0;
            for (Text val : values) {
                docCount++;
            }

            double idf = Math.log10((double) totalDocs / (1 + docCount));

            for (Text val : values) {
                String[] docTf = val.toString().split("=");
                String docId = docTf[0];
                double tf = Double.parseDouble(docTf[1]);
                double tfidfValue = tf * idf;
                tfidf.set(tfidfValue);
                context.write(new Text(key.toString() + "@" + docId), tfidf);
            }
        }
    }
}
