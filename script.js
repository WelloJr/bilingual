package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFMapper extends Mapper<Object, Text, Text, Text> {

    private Text term = new Text();
    private Text docAndCount = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the line for term and document data
        String line = value.toString().trim();

        // Check if the line is properly formatted
        if (line.startsWith("<") && line.endsWith(">")) {
            String content = line.substring(1, line.length() - 1).trim();  // Remove the '< >'
            String[] parts = content.split("\\s+", 2);  // Split the term and document data

            String termText = parts[0];
            String docData = parts[1];

            // Split the docData by ';' to handle multiple documents
            String[] docs = docData.split(";");
            for (String doc : docs) {
                String docId = doc.split(":")[0].trim();  // Extract docId
                int count = doc.split(":")[1].split(",").length;  // Count positions
                term.set(termText);  // Set the term
                docAndCount.set(docId + ":" + count);  // Set docId and count
                context.write(term, docAndCount);  // Emit the term and document with its count
            }
        }
    }
}
