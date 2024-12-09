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

        // StringBuilder to hold the final output
        StringBuilder output = new StringBuilder();

        // Iterate through all documents (doc1 to doc10)
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            // Get the frequency from the map or 0 if the term is not in the document
            int freq = docFrequencyMap.containsKey(docKey) ? docFrequencyMap.get(docKey) : 0;
            output.append(docKey).append(":").append(freq).append("; ");
        }

        // Remove the trailing space and write the output
        result.set(output.toString().trim());
        context.write(key, result);
    }
}
