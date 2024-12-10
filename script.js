package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

@Override
protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    double idf = 0.0;
    StringBuilder output = new StringBuilder();

    for (Text val : values) {
        String[] docData = val.toString().split(";");
        
        // Ensure the docData has values before processing
        if (docData.length > 0) {
            for (String doc : docData) {
                String[] docParts = doc.split(":");
                if (docParts.length == 2) {  // Ensure that there are two parts (docId, tf value)
                    String docId = docParts[0].trim();
                    int tf = Integer.parseInt(docParts[1].trim());
                    
                    // Compute TF-IDF for each document
                    double tfIdf = (1 + Math.log10(tf)) * idf; // Assuming IDF is already computed
                    output.append(docId).append(": ").append(tfIdf).append("; ");
                }
            }
        }
    }

    result.set(output.toString().trim());
    context.write(key, result);
}
