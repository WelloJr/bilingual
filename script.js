package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Count unique documents where the term appears
        for (@SuppressWarnings("unused") Text ignored : values) {
            docCount++;
        }

        int totalDocs = 10; // Total number of documents
        double idf = docCount > 0 ? Math.log10((double) totalDocs / docCount) : 0;

        // Emit the term with DF and IDF
        result.set(docCount + " | " + idf);
        context.write(key, result);
    }
}
