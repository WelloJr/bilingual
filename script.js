package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text term = new Text();
    private Text docAndTfIdf = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\t", 2);
        
        // Parse the term and the corresponding TF and IDF values
        String termText = parts[0];
        String docAndValues = parts[1];
        
        // Format: doc1: TF; doc2: TF; ...
        // Extract TF and IDF from the values
        String[] docData = docAndValues.split("; ");
        
        for (String doc : docData) {
            String[] docParts = doc.split(":");
            String docId = docParts[0];
            double tf = Double.parseDouble(docParts[1]);
            double idf = 0.0;

            // Extract the IDF value from the IDF output file (assuming IDF value is stored elsewhere)
            // You need to get the corresponding IDF value for each term from IDF output.

            // For simplicity, assuming we get the IDF value (this part needs to be adjusted based on your project setup)

            // Calculate TF-IDF: TF * IDF
            double tfIdf = tf * idf;
            
            // Emit the term and the document with its TF-IDF value
            term.set(termText);
            docAndTfIdf.set(docId + ":" + tfIdf);
            context.write(term, docAndTfIdf);
        }
    }
}
