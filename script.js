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

public class PartTwoDriver extends ToolRunner implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PartTwoDriver <input path> <intermediate path> <output path>");
            return -1;
        }

        String inputPath = args[0]; // Input file path
        String intermediatePathTF = args[1] + "_tf"; // Intermediate output for TF
        String intermediatePathIDF = args[1] + "_idf"; // Intermediate output for IDF
        String outputPathTFIDF = args[2]; // Final output for TF-IDF

        FileSystem fs = FileSystem.get(conf);

        // Step 1: Term Frequency (TF) Job
        Job tfJob = Job.getInstance(conf, "Term Frequency Calculation");
        tfJob.setJarByClass(PartTwoDriver.class);
        tfJob.setMapperClass(TFMapper.class);
        tfJob.setReducerClass(TFReducer.class);
        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(tfJob, new Path(intermediatePathTF));
        if (fs.exists(new Path(intermediatePathTF))) fs.delete(new Path(intermediatePathTF), true);

        if (!tfJob.waitForCompletion(true)) return 1;

        // Step 2: Inverse Document Frequency (IDF) Job
        Job idfJob = Job.getInstance(conf, "Inverse Document Frequency Calculation");
        idfJob.setJarByClass(PartTwoDriver.class);
        idfJob.setMapperClass(IDFMapper.class);
        idfJob.setReducerClass(IDFReducer.class);
        idfJob.setOutputKeyClass(Text.class);
        idfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(idfJob, new Path(intermediatePathTF));
        FileOutputFormat.setOutputPath(idfJob, new Path(intermediatePathIDF));
        if (fs.exists(new Path(intermediatePathIDF))) fs.delete(new Path(intermediatePathIDF), true);

        if (!idfJob.waitForCompletion(true)) return 1;

        // Step 3: TF-IDF Calculation Job
        Job tfidfJob = Job.getInstance(conf, "TF-IDF Calculation");
        tfidfJob.setJarByClass(PartTwoDriver.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfidfJob, new Path(intermediatePathIDF));
        FileOutputFormat.setOutputPath(tfidfJob, new Path(outputPathTFIDF));
        if (fs.exists(new Path(outputPathTFIDF))) fs.delete(new Path(outputPathTFIDF), true);

        if (!tfidfJob.waitForCompletion(true)) return 1;

        return 0; // Successfully completed all steps
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
