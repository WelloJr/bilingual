package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class PositionalIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text term = new Text();
    private Text docPos = new Text();
    private String docID;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Extract the document ID from the file name
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        docID = fileName.split("\\.")[0];  // Extract the file name without the extension (e.g., "1" from "1.txt")
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the content of the document
        String content = value.toString();

        // Split the content into words (terms)
        String[] words = content.split("\\s+");

        // Emit each word with its position in the document
        for (int i = 0; i < words.length; i++) {
            term.set(words[i].toLowerCase().replaceAll("[^a-zA-Z0-9]", "")); // Normalize word by removing non-alphanumeric characters
            docPos.set(docID + ":" + (i + 1)); // Format: docID:position (e.g., "1:1", "1:2")
            context.write(term, docPos); // Emit the term and its position
        }
    }
}
