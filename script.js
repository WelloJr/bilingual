package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class IDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text term = new Text();
    private Text docId = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\t", 2); // Expecting "term<TAB>docData"
        if (parts.length < 2) {
            return; // Skip malformed lines
        }

        String termText = parts[0];
        String docData = parts[1];

        // Use a set to emit each document ID only once per term
        HashSet<String> uniqueDocs = new HashSet<>();
        String[] docs = docData.split(";");
        for (String doc : docs) {
            String docIdPart = doc.split(":")[0].trim(); // Extract doc ID
            uniqueDocs.add(docIdPart);
        }

        for (String uniqueDoc : uniqueDocs) {
            term.set(termText);
            docId.set(uniqueDoc);
            context.write(term, docId);
        }
    }
}
