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
        for (Text ignored : values) {
            docCount++;
        }

        // Total number of documents (adjust based on your dataset)
        int totalDocuments = 10;

        // Avoid division by zero; compute IDF only if docCount > 0
        double idf = 0.0;
        if (docCount > 0) {
            idf = Math.log10((double) totalDocuments / docCount);
        }

        // Set the result to the computed IDF value
        result.set(String.valueOf(idf));
        context.write(key, result);  // Emit the term and its IDF value
    }
}
