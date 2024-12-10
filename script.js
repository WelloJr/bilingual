package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // StringBuilder to accumulate the result
        StringBuilder output = new StringBuilder();

        // Iterate through all the documents for the current term
        for (Text val : values) {
            String docAndTfIdf = val.toString().trim();
            output.append(docAndTfIdf).append("; ");
        }

        // Remove the trailing space and write the output
        result.set(output.toString().trim());
        context.write(key, result);  // Emit the term and its TF-IDF values for each document
    }
}
