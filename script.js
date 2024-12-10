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
        // Create a map to hold the TF and IDF values for each document
        Map<String, Double> tfMap = new HashMap<>();
        double idf = 0.0;

        // First, split values into TF and IDF
        for (Text val : values) {
            String[] parts = val.toString().split(":");

            // If the value is in the form docId:TF, store it in the TF map
            if (parts.length == 2) {
                String docId = parts[0].trim();
                double tf = Double.parseDouble(parts[1].trim());
                tfMap.put(docId, tf);
            }
            // Otherwise, it's the IDF value, so store it
            else if (parts.length == 1) {
                idf = Double.parseDouble(parts[0].trim());
            }
        }

        // Now calculate the TF-IDF for each document
        StringBuilder output = new StringBuilder();
        for (Map.Entry<String, Double> entry : tfMap.entrySet()) {
            String docId = entry.getKey();
            double tf = entry.getValue();
            double tfIdf = tf * idf; // Calculate TF-IDF
            output.append(docId).append(":").append(tfIdf).append("; ");
        }

        // Write the result
        result.set(output.toString().trim());
        context.write(key, result);  // Emit the term and its TF-IDF for each document
    }
}
