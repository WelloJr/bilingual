package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Map to store TF-IDF scores for documents
        Map<String, Double> tfIdfMap = new HashMap<>();

        // Retrieve IDF for the term from the values
        double idf = 0.0;
        Map<String, Integer> tfMap = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split(";");
            for (String docEntry : parts) {
                String[] docParts = docEntry.split(":");
                if (docParts.length == 2) {
                    String docId = docParts[0].trim();
                    int tf = Integer.parseInt(docParts[1].trim());
                    tfMap.put(docId, tf);
                } else {
                    idf = Double.parseDouble(docEntry.trim());
                }
            }
        }

        // Calculate TF-IDF for all documents
        StringBuilder output = new StringBuilder();
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            int tf = tfMap.containsKey(docKey) ? tfMap.get(docKey) : 0; // Replace getOrDefault
            double tfIdf = tf * idf;
            tfIdfMap.put(docKey, tfIdf);
            output.append(docKey).append(":").append(tfIdf).append("; ");
        }

        // Write the result for the term
        result.set(output.toString().trim());
        context.write(key, result);
    }
}
