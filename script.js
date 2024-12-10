package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        for (Text val : values) {
            if (val != null) {
                docCount++;
            }
        }

        int totalDocuments = 10; // Adjust if the dataset size changes
        double idf = 0.0;
        if (docCount > 0) {
            idf = Math.log10((double) totalDocuments / docCount);  // Compute IDF
        }

        result.set(String.valueOf(idf));
        context.write(key, result);
    }
}
