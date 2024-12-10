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
        double idf = 0.0;
        Map<String, Integer> termFrequencies = new HashMap<>();

        // Separate TF and IDF inputs
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length == 2 && parts[0].startsWith("idf")) {
                idf = Double.parseDouble(parts[1].trim());
            } else {
                String docId = parts[0].trim();
                int tf = Integer.parseInt(parts[1].trim());
                termFrequencies.put(docId, tf);
            }
        }

        // Multiply TF by IDF for each document
        StringBuilder output = new StringBuilder();
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            int tf = termFrequencies.getOrDefault(docKey, 0);
            double tfidf = tf * idf;
            output.append(docKey).append(":").append(String.format("%.6f", tfidf)).append("; ");
        }

        result.set(output.toString().trim());
        context.write(key, result);
    }
}
