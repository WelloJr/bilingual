package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Count the number of unique documents
        for (Text ignored : values) {
            docCount++;
        }

        // Compute IDF using log10(N / DF), assuming N = 10 documents
        double idf = Math.log10(10.0 / docCount);

        // Format output to include DF for debugging purposes
        String output = docCount + " | " + idf;
        result.set(output);

        context.write(key, result);
    }
}
