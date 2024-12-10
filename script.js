package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Iterate over the values to count the documents
        for (@SuppressWarnings("unused") Text value : values) {
            docCount++;
        }

        int totalDocs = 10; // Total number of documents
        double idf = Math.log10((double) totalDocs / docCount);

        // Set the output as DF | IDF
        result.set(docCount + " | " + idf);
        context.write(key, result);
    }
}
