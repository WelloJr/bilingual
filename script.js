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
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PartTwoDriver <input path> <output path>");
            return -1;
        }

        String inputPath = args[0];  // Input file path from Part 1 (positional index file)
        String outputPath = args[2];  // Output path where TF, IDF, and TF-IDF will be stored

        // Get the Hadoop configuration and FileSystem object
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Clean up the output directory if it already exists
        Path outputPathTFIDF = new Path(outputPath);
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
        FileOutputFormat.setOutputPath(tfJob, outputPathTFIDF);

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

        FileInputFormat.addInputPath(idfJob, outputPathTFIDF);  // Read from TF output
        FileOutputFormat.setOutputPath(idfJob, outputPathTFIDF);

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

        FileInputFormat.addInputPath(tfidfJob, outputPathTFIDF);  // Read from IDF output
        FileOutputFormat.setOutputPath(tfidfJob, outputPathTFIDF);

        if (!tfidfJob.waitForCompletion(true)) {
            return 1;  // If TF-IDF Job fails, stop the execution
        }

        return 0;  // All jobs completed successfully
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartTwoDriver(), args);
        System.exit(exitCode);
    }
}
