package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Count the number of documents containing the term
        for (Text val : values) {
            docCount++;  // Increment for each document
        }

        // Total number of documents (adjust this based on your dataset size)
        int totalDocuments = 10;

        // Compute IDF: Avoid division by zero by ensuring docCount > 0
        double idf = 0.0;
        if (docCount > 0) {
            idf = Math.log10((double) totalDocuments / docCount);
        }

        // Write the term and its IDF value
        result.set(String.valueOf(idf));
        context.write(key, result);
    }
}
