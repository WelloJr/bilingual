package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double idf = 0.0;  // Placeholder for IDF
        StringBuilder output = new StringBuilder();

        for (Text val : values) {
            String[] parts = val.toString().split("\\|");
            idf = Double.parseDouble(parts[0]);  // Parse the IDF value

            // Process TF data
            String[] docData = parts[1].split(";");
            for (String doc : docData) {
                String[] docParts = doc.split(":");
                String docId = docParts[0].trim();
                int tf = Integer.parseInt(docParts[1].trim());

                // Compute TF-IDF
                double tfIdf = (1 + Math.log10(tf)) * idf;
                output.append(docId).append(":").append(tfIdf).append("; ");
            }
        }

        result.set(output.toString().trim());
        context.write(key, result);
    }
}
