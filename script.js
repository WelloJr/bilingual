package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Count how many documents contain this term
        for (Text ignored : values) {
            docCount++; // Simply count the number of values (documents)
        }

        int totalDocuments = 10;  // Assuming 10 documents in the corpus
        double idf = 0.0;
        if (docCount > 0) {
            idf = Math.log10((double) totalDocuments / docCount);  // Calculate IDF
        }

        result.set(String.valueOf(idf));
        context.write(key, result);  // Emit the term and its IDF value
    }
}
