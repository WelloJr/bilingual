package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Iterate through the values to count the number of documents
        for (Text value : values) {
            docCount++;  // We are counting the number of documents, no need to use 'value'
        }

        // Compute IDF using log10(N / DF), assuming N = 10 documents
        double idf = Math.log10(10.0 / docCount);  // DF is the document count (docCount)

        // Set the result as the IDF value
        result.set(String.valueOf(idf));

        // Write the term and its IDF value to the context
        context.write(key, result);
    }
}
