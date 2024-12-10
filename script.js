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

        // Use an iterator to iterate through the values
        Iterator<Text> iterator = values.iterator();
        
        // Count how many unique documents contain the term
        while (iterator.hasNext()) {
            iterator.next(); // Move to the next value (document)
            docCount++;  // Increment the document count for this term
        }

        int totalDocuments = 10;  // Assuming there are 10 documents in total
        double idf = 0.0;
        if (docCount > 0) {
            idf = Math.log10((double) totalDocuments / docCount);  // Calculate IDF
        }

        result.set(String.valueOf(idf));
        context.write(key, result);  // Emit the term and its IDF value
    }
}
