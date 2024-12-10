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
        // Map to hold term frequencies per document
        Map<String, Integer> docFrequencyMap = new HashMap<>();

        // Populate the map with actual term frequencies
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String docId = parts[0].trim();
            int count = Integer.parseInt(parts[1].trim());
            docFrequencyMap.put(docId, count);
        }

        // Prepare output with all documents, including missing ones
        StringBuilder output = new StringBuilder();
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            int freq = docFrequencyMap.getOrDefault(docKey, 0); // Default to 0 if document is missing
            output.append(docKey).append(":").append(freq).append("; ");
        }

        result.set(output.toString().trim());
        context.write(key, result);
    }
}
