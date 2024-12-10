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
        // Map to store term frequencies across all documents
        Map<String, Integer> docFrequencyMap = new HashMap<>();

        // Populate the map with actual term frequencies from the values
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String docId = parts[0].trim();
            int count = Integer.parseInt(parts[1].trim());
            docFrequencyMap.put(docId, count);
        }

        // Include all documents in the output, setting frequency to 0 where absent
        StringBuilder output = new StringBuilder();
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            int freq = docFrequencyMap.containsKey(docKey) ? docFrequencyMap.get(docKey) : 0; // Replace getOrDefault
            output.append(docKey).append(":").append(freq).append("; ");
        }

        // Set and write the result
        result.set(output.toString().trim());
        context.write(key, result);
    }
}
