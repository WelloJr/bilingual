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
        String[] parts = line.split("\\s+", 2);
        String termText = parts[0];
        String docData = parts[1];

        // Use a HashSet to avoid duplicate documents
        HashSet<String> uniqueDocs = new HashSet<>();
        String[] docs = docData.split(";");
        for (String doc : docs) {
            String docIdPart = doc.split(":")[0];
            uniqueDocs.add(docIdPart);
        }

        // Emit each term and document ID pair for further processing
        for (String uniqueDoc : uniqueDocs) {
            term.set(termText);
            docId.set(uniqueDoc);
            context.write(term, docId);
        }
    }
}
