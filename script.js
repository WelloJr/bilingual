package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text term = new Text();
    private Text docAndTfIdf = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Each line is in the form of term \t doc1:TF ; doc2:TF ; ...
        String line = value.toString().trim();
        String[] parts = line.split("\\t");

        // Ensure that the line has both the term and document frequency data
        if (parts.length < 2) {
            return;  // Skip lines that don't have both term and doc data
        }

        String termText = parts[0]; // The term (e.g., "angels")
        String docData = parts[1];  // The docID and corresponding TF values

        String[] docs = docData.split(";");

        // For each document for this term, get the TF and IDF
        for (String doc : docs) {
            String[] docParts = doc.split(":");
            
            // Ensure we have both docId and TF value
            if (docParts.length != 2) {
                continue;  // Skip any doc data that's incorrectly formatted
            }

            String docId = docParts[0].trim();
            double tf = Double.parseDouble(docParts[1].trim());

            // Get the IDF for this term (assumes IDF is available in context)
            double idf = 0.0;  // Placeholder, IDF value should come from the IDF output

            double tfIdf = tf * idf;  // Multiply TF and IDF to get TF-IDF

            // Emit term and doc with the corresponding TF-IDF value
            term.set(termText);
            docAndTfIdf.set(docId + ":" + tfIdf);
            context.write(term, docAndTfIdf);
        }
    }
}
