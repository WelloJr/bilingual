package com.example.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDF {
    // TF-IDF Mapper
    public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split input line into parts
            String[] parts = value.toString().split("\\t");
            if (parts.length < 2) {
                // Skip malformed input
                return;
            }

            // Extract word@docId and TF value
            String[] wordDoc = parts[0].split("@");
            if (wordDoc.length < 2) {
                // Skip malformed input
                return;
            }
            String word = wordDoc[0];
            String docId = wordDoc[1];
            String tf = parts[1];

            // Emit word as key, and docId=tf as value
            context.write(new Text(word), new Text(docId + "=" + tf));
        }
    }

    // TF-IDF Reducer
    public static class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable tfidf = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalDocs = 10; // Example: total number of documents
            int docCount = 0;

            // Collect all values for this key
            StringBuilder allValues = new StringBuilder();
            for (Text val : values) {
                allValues.append(val.toString()).append(",");
                docCount++;
            }

            // Calculate IDF
            double idf = Math.log10((double) totalDocs / (1 + docCount));

            // Split back into individual TF values and calculate TF-IDF
            String[] tfValues = allValues.toString().split(",");
            for (String tfValue : tfValues) {
                if (tfValue.isEmpty()) continue; // Skip empty values

                String[] docTf = tfValue.split("=");
                if (docTf.length < 2) continue; // Skip malformed input

                String docId = docTf[0];
                double tf = Double.parseDouble(docTf[1]);
                double tfidfValue = tf * idf;

                // Emit final TF-IDF value
                tfidf.set(tfidfValue);
                context.write(new Text(key.toString() + "@" + docId), tfidf);
            }
        }
    }
}
