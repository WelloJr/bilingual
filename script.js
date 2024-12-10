package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Count the number of documents where the term appears
        for (@SuppressWarnings("unused") Text ignored : values) {
            docCount++;
        }

        // Calculate IDF using the formula: log(N / DF)
        int totalDocs = 10; // Total number of documents
        double idf = Math.log10((double) totalDocs / docCount);

        // Emit the term with DF and IDF
        result.set(docCount + " | " + idf);
        context.write(key, result);
    }
}
