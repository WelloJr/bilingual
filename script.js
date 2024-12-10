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

public class PartTwoDriver extends org.apache.hadoop.conf.Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PartTwoDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PartTwoDriver <input path> <intermediate path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Cleanup logic: Ensure intermediate and output directories are cleared before starting
        Path intermediatePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        if (fs.exists(intermediatePath)) {
            fs.delete(intermediatePath, true);
        }
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Step 1: Run TF Job
        Job tfJob = Job.getInstance(conf, "Term Frequency Calculation");
        tfJob.setJarByClass(PartTwoDriver.class);
        tfJob.setMapperClass(TFMapper.class);
        tfJob.setReducerClass(TFReducer.class);
        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tfJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(tfJob, new Path(args[1] + "_tf"));
        if (!tfJob.waitForCompletion(true)) {
            return 1;
        }

        // Step 2: Run IDF Job
        Job idfJob = Job.getInstance(conf, "Inverse Document Frequency Calculation");
        idfJob.setJarByClass(PartTwoDriver.class);
        idfJob.setMapperClass(IDFMapper.class);
        idfJob.setReducerClass(IDFReducer.class);
        idfJob.setOutputKeyClass(Text.class);
        idfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(idfJob, new Path(args[1] + "_tf"));
        FileOutputFormat.setOutputPath(idfJob, new Path(args[1] + "_idf"));
        if (!idfJob.waitForCompletion(true)) {
            return 1;
        }

        // Step 3: Run TFIDF Job
        Job tfidfJob = Job.getInstance(conf, "TFIDF Calculation");
        tfidfJob.setJarByClass(PartTwoDriver.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tfidfJob, new Path(args[1] + "_idf"));
        FileOutputFormat.setOutputPath(tfidfJob, new Path(args[2]));
        return tfidfJob.waitForCompletion(true) ? 0 : 1;
    }
}
