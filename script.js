package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

    private Text term = new Text();
    private Text idfAndTf = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\\t", 2);  // Split by tab to separate term and data
        term.set(parts[0]);  // Set the term
        idfAndTf.set(parts[1]);  // Set the associated IDF and TF data
        context.write(term, idfAndTf);  // Emit the term with its data
    }
}
