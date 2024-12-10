package part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartTwoDriver implements Tool, Configurable {

    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartTwoDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PartTwoDriver <input path> <intermediate path> <output path>");
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path intermediatePathTF = new Path(args[1] + "_tf");
        Path intermediatePathIDF = new Path(args[1] + "_idf");
        Path outputPath = new Path(args[2]);

        // Step 1: TF Job
        Job tfJob = Job.getInstance(conf, "TF Calculation");
        tfJob.setJarByClass(PartTwoDriver.class);
        tfJob.setMapperClass(TFMapper.class);
        tfJob.setReducerClass(TFReducer.class);
        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfJob, inputPath);
        FileOutputFormat.setOutputPath(tfJob, intermediatePathTF);

        if (!tfJob.waitForCompletion(true)) {
            return 1;
        }

        // Step 2: IDF Job
        Job idfJob = Job.getInstance(conf, "IDF Calculation");
        idfJob.setJarByClass(PartTwoDriver.class);
        idfJob.setMapperClass(IDFMapper.class);
        idfJob.setReducerClass(IDFReducer.class);
        idfJob.setOutputKeyClass(Text.class);
        idfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(idfJob, intermediatePathTF);
        FileOutputFormat.setOutputPath(idfJob, intermediatePathIDF);

        if (!idfJob.waitForCompletion(true)) {
            return 1;
        }

        // Step 3: TF-IDF Job
        Job tfidfJob = Job.getInstance(conf, "TF-IDF Calculation");
        tfidfJob.setJarByClass(PartTwoDriver.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfidfJob, intermediatePathIDF);
        FileOutputFormat.setOutputPath(tfidfJob, outputPath);

        return tfidfJob.waitForCompletion(true) ? 0 : 1;
    }
}
