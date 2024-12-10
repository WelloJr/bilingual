package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Manually iterate over the values to count the number of documents (DF)
        for (Text value : values) {
            // We don't need to do anything with the value here
            docCount++;
        }

        // Compute IDF using the formula: IDF = log(N / DF)
        double idf = Math.log10(10.0 / docCount);  // Assume 10 total documents

        // Set the IDF result for the term
        result.set(String.valueOf(idf));

        // Output the term and its IDF value
        context.write(key, result);
    }
}
