package tf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TFIDF {
    
    // Mapper Class
    public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text docAndTf = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input format: word@docId \t termFrequency
            String[] parts = value.toString().split("\\t");
            if (parts.length < 2) return;

            String[] wordAndDoc = parts[0].split("@");
            if (wordAndDoc.length < 2) return;

            word.set(wordAndDoc[0]);               // Extract the word
            docAndTf.set(wordAndDoc[1] + "=" + parts[1]); // docId=termFrequency
            context.write(word, docAndTf);        // Emit (word, docId=termFrequency)
        }
    }

    // Reducer Class
    public static class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable tfidfValue = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalDocs = 10; // Update this to match the total number of documents in your dataset
            int docFrequency = 0;

            // Count the number of documents the term appears in
            for (Text val : values) {
                docFrequency++;
            }

            double idf = Math.log10((double) totalDocs / (1 + docFrequency));

            // For each document, calculate TF-IDF
            for (Text val : values) {
                String[] docAndTf = val.toString().split("=");
                if (docAndTf.length < 2) continue;

                String docId = docAndTf[0];
                double tf = Double.parseDouble(docAndTf[1]);
                double tfidf = tf * idf;

                tfidfValue.set(tfidf);
                context.write(new Text(key.toString() + "@" + docId), tfidfValue); // Emit word@docId, TF-IDF
            }
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TFIDF <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TF-IDF Calculation");
        job.setJarByClass(TFIDF.class);

        // Set Mapper and Reducer
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        // Set Output Key and Value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
