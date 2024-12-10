package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Iterate over the values, just counting the occurrences
        for (Text ignored : values) {
            docCount++;  // Increment count for each document
        }

        // Calculate IDF (assuming 10 total documents, replace if needed)
        double idf = Math.log10(10.0 / docCount); // Assuming N = 10 documents

        // Set the IDF result for the current term
        result.set(String.valueOf(idf));

        // Output the term and its IDF value
        context.write(key, result);
    }
}
