package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TFMapper extends Mapper<Object, Text, Text, Text> {

    private Text term = new Text();
    private Text docAndCount = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the positional index line
        String line = value.toString().trim();
        if (line.startsWith("<") && line.endsWith(">")) {
            String content = line.substring(1, line.length() - 1).trim();
            String[] parts = content.split("\\s+", 2);
            String termText = parts[0];
            String docData = parts[1];

            // Extract docId and positions
            String[] docs = docData.split(";");
            for (String doc : docs) {
                String docId = doc.split(":")[0].trim();
                int count = doc.split(":")[1].split(",").length;
                term.set(termText);
                docAndCount.set(docId + ":" + count);
                context.write(term, docAndCount);
            }
        }
    }
}
