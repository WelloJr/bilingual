package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text term = new Text();
    private Text docAndTf = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\| DF:");
        if (parts.length < 2) {
            return; // Skip malformed lines
        }

        term.set(parts[0].trim()); // Term
        docAndTf.set(parts[1].trim()); // TF and IDF data
        context.write(term, docAndTf);
    }
}
