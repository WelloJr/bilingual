package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text term = new Text();
    private Text docId = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\s+", 2);
        String termText = parts[0];
        String docData = parts[1];

        // Split the docData to get individual document frequencies
        String[] docs = docData.split(";");

        // For each document that contains the term, emit the term and the document ID
        for (String doc : docs) {
            String docIdPart = doc.split(":")[0];
            term.set(termText);
            docId.set(docIdPart);
            context.write(term, docId); // Emit term and docId
        }
    }
}
