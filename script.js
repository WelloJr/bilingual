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
        // Map to hold the term frequencies for each document
        Map<String, Integer> docFrequencyMap = new HashMap<>();

        // Iterate through the values to populate the docFrequencyMap
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String docId = parts[0].trim();
            int count = Integer.parseInt(parts[1].trim());
            docFrequencyMap.put(docId, count);  // Store frequency for each document
        }

        // StringBuilder to accumulate the output for all documents (doc1 to doc10)
        StringBuilder output = new StringBuilder();
        
        // Iterate over all 10 documents
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            
            // If the document exists in the map, get its frequency; otherwise, default to 0
            int freq = 0;
            if (docFrequencyMap.containsKey(docKey)) {
                freq = docFrequencyMap.get(docKey);  // Get the frequency of the term in the document
            }
            
            // Append the document ID and its term frequency (or 0 if not present)
            output.append(docKey).append(":").append(freq).append("; ");
        }

        // Set the final output and write it to the context
        result.set(output.toString().trim());
        context.write(key, result);
    }
}
