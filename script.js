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

        String[] docs = docData.split(";");
        for (String doc : docs) {
            String[] docParts = doc.trim().split(":");
            if (docParts.length == 2) {
                String docIdPart = docParts[0].trim();
                term.set(termText);
                docId.set(docIdPart);
                context.write(term, docId);
            }
        }
    }
}
