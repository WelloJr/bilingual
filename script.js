package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Count the number of documents that contain the term
        for (Text val : values) {
            if (val != null) {
                docCount++;  // Count each document where the term appears
            }
        }

        int totalDocuments = 10;  // Assuming 10 documents in total
        double idf = 0.0;
        if (docCount > 0) {
            idf = Math.log10((double) totalDocuments / docCount);  // Compute IDF
        }

        result.set(String.valueOf(idf));  // Store the IDF value
        context.write(key, result);  // Output the term and its IDF
    }
}
