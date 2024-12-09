package part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartTwoDriver extends Configured implements Tool {

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

        // TF Job
        Job tfJob = Job.getInstance(conf, "TF Calculation");
        tfJob.setJarByClass(PartTwoDriver.class);
        tfJob.setMapperClass(TFMapper.class);
        tfJob.setReducerClass(TFReducer.class);
        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tfJob, new Path(args[0]));
        Path tfOutputPath = new Path(args[1] + "_tf");
        FileOutputFormat.setOutputPath(tfJob, tfOutputPath);
        if (fs.exists(tfOutputPath)) fs.delete(tfOutputPath, true);
        if (!tfJob.waitForCompletion(true)) return 1;

        // IDF Job
        Job idfJob = Job.getInstance(conf, "IDF Calculation");
        idfJob.setJarByClass(PartTwoDriver.class);
        idfJob.setMapperClass(IDFMapper.class);
        idfJob.setReducerClass(IDFReducer.class);
        idfJob.setOutputKeyClass(Text.class);
        idfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(idfJob, tfOutputPath);
        Path idfOutputPath = new Path(args[1] + "_idf");
        FileOutputFormat.setOutputPath(idfOutputPath);
        if (fs.exists(idfOutputPath)) fs.delete(idfOutputPath, true);
        if (!idfJob.waitForCompletion(true)) return 1;

        // TF-IDF Job
        Job tfidfJob = Job.getInstance(conf, "TF-IDF Calculation");
        tfidfJob.setJarByClass(PartTwoDriver.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tfidfJob, idfOutputPath);
        Path tfidfOutputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(tfidfOutputPath);
        if (fs.exists(tfidfOutputPath)) fs.delete(tfidfOutputPath, true);
        return tfidfJob.waitForCompletion(true) ? 0 : 1;
    }
}
