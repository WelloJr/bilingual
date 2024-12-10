package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        // Use an explicit iterator to avoid unused variable warnings
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            iterator.next();  // Move to the next value (we don't use the value itself)
            docCount++;       // Count the document
        }

        // Calculate IDF (assuming N = 10 documents)
        double idf = Math.log10(10.0 / docCount); // DF = docCount

        // Set the result as the IDF value
        result.set(String.valueOf(idf));

        // Write the term and its IDF value to the context
        context.write(key, result);
    }
}
