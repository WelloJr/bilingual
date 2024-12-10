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
        String[] parts = line.split("\\| DF:");
        if (parts.length < 2) {
            return; // Skip malformed lines
        }

        String termData = parts[0].trim();
        String dfData = parts[1].trim();
        int df = Integer.parseInt(dfData);

        if (df == 0) {
            return; // Ignore terms with DF = 0
        }

        String[] termParts = termData.split("\\t", 2);
        if (termParts.length < 2) {
            return;
        }

        term.set(termParts[0].trim()); // Term
        docId.set(termParts[1].trim()); // DocId and TF data
        context.write(term, docId);
    }
}
