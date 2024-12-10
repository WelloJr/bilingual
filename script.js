package part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartTwoDriver implements Tool {

    private Configuration conf; // Declare Configuration object

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PartTwoDriver <input path> <intermediate path> <output path>");
            return -1;
        }

        String inputPath = args[0];  // Input file path from Part 1 (positional index file)
        String intermediatePath = args[1]; // Intermediate output directory for TF and IDF
        String outputPath = args[2];  // Final output directory for TF-IDF results

        // Get the Hadoop configuration and FileSystem object
        FileSystem fs = FileSystem.get(conf);

        // Clean up the output directories if they already exist
        Path intermediatePathTF = new Path(intermediatePath + "_tf");
        Path intermediatePathIDF = new Path(intermediatePath + "_idf");
        Path outputPathTFIDF = new Path(outputPath);

        if (fs.exists(intermediatePathTF)) {
            fs.delete(intermediatePathTF, true);  // Delete if already exists
        }
        if (fs.exists(intermediatePathIDF)) {
            fs.delete(intermediatePathIDF, true);  // Delete if already exists
        }
        if (fs.exists(outputPathTFIDF)) {
            fs.delete(outputPathTFIDF, true);  // Delete if already exists
        }

        // Run TF Job
        Job tfJob = Job.getInstance(conf, "Term Frequency Calculation");
        tfJob.setJarByClass(PartTwoDriver.class);
        tfJob.setMapperClass(TFMapper.class);
        tfJob.setReducerClass(TFReducer.class);
        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(tfJob, intermediatePathTF);

        if (!tfJob.waitForCompletion(true)) {
            return 1;  // If TF Job fails, stop the execution
        }

        // Run IDF Job using output of TF job
        Job idfJob = Job.getInstance(conf, "Inverse Document Frequency Calculation");
        idfJob.setJarByClass(PartTwoDriver.class);
        idfJob.setMapperClass(IDFMapper.class);
        idfJob.setReducerClass(IDFReducer.class);
        idfJob.setOutputKeyClass(Text.class);
        idfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(idfJob, intermediatePathTF);  // Read from TF output
        FileOutputFormat.setOutputPath(idfJob, intermediatePathIDF);

        if (!idfJob.waitForCompletion(true)) {
            return 1;  // If IDF Job fails, stop the execution
        }

        // Run TF-IDF Job using output of IDF job
        Job tfidfJob = Job.getInstance(conf, "TF-IDF Calculation");
        tfidfJob.setJarByClass(PartTwoDriver.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfidfJob, intermediatePathIDF);  // Read from IDF output
        FileOutputFormat.setOutputPath(tfidfJob, outputPathTFIDF);

        if (!tfidfJob.waitForCompletion(true)) {
            return 1;  // If TF-IDF Job fails, stop the execution
        }

        return 0;  // All jobs completed successfully
    }

    // Directly setting and getting the configuration
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartTwoDriver(), args);
        System.exit(exitCode);
    }
}
