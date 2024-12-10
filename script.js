package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Map to hold the document frequencies for the current term
        Map<String, Integer> docFrequencyMap = new HashMap<>();

        // Populate map with actual term frequencies from the values
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String docId = parts[0].trim();
            int count = Integer.parseInt(parts[1].trim());
            docFrequencyMap.put(docId, count);
        }

        // StringBuilder to hold the final TF output
        StringBuilder output = new StringBuilder();
        for (Map.Entry<String, Integer> entry : docFrequencyMap.entrySet()) {
            output.append(entry.getKey()).append(":").append(entry.getValue()).append("; ");
        }

        // Remove the trailing space and write the output
        result.set(output.toString().trim());
        context.write(key, result);
    }
}
