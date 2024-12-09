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
        term.set(parts[0]);
        docAndTfIdf.set(parts[1]);
        context.write(term, docAndTfIdf);
    }
}
