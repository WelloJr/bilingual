package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Count the number of unique documents for each term
        for (Text val : values) {
            docCount++;  // Count each document where the term appears
        }

        int totalDocuments = 10;  // Total number of documents in the corpus
        double idf = 0.0;
        if (docCount > 0) {
            idf = Math.log10((double) totalDocuments / docCount);  // Calculate IDF
        }

        result.set(String.valueOf(idf));
        context.write(key, result);  // Emit the term and its IDF value
    }
}
