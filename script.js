package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Use a set to track unique document IDs
        HashSet<String> uniqueDocs = new HashSet<>();

        for (Text val : values) {
            uniqueDocs.add(val.toString().trim());
        }

        // Compute Document Frequency (DF)
        int df = uniqueDocs.size();
        double idf = Math.log10(10.0 / (double) df);  // Assuming 10 documents

        // Output DF and IDF for verification
        result.set(df + " | " + idf);
        context.write(key, result);
    }
}
