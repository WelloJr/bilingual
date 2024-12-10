package part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartTwoDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // TF Job
        Job tfJob = Job.getInstance(conf, "TF Calculation");
        tfJob.setJarByClass(PartTwoDriver.class);
        tfJob.setMapperClass(TFMapper.class);
        tfJob.setReducerClass(TFReducer.class);
        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfJob, new Path(args[0]));
        Path tfOutput = new Path(args[1] + "_tf");
        FileOutputFormat.setOutputPath(tfJob, tfOutput);
        if (fs.exists(tfOutput)) fs.delete(tfOutput, true);
        tfJob.waitForCompletion(true);

        // IDF Job
        Job idfJob = Job.getInstance(conf, "IDF Calculation");
        idfJob.setJarByClass(PartTwoDriver.class);
        idfJob.setMapperClass(IDFMapper.class);
        idfJob.setReducerClass(IDFReducer.class);
        idfJob.setOutputKeyClass(Text.class);
        idfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(idfJob, tfOutput);
        Path idfOutput = new Path(args[1] + "_idf");
        FileOutputFormat.setOutputPath(idfJob, idfOutput);
        if (fs.exists(idfOutput)) fs.delete(idfOutput, true);
        idfJob.waitForCompletion(true);

        // TFIDF Job
        Job tfidfJob = Job.getInstance(conf, "TFIDF Calculation");
        tfidfJob.setJarByClass(PartTwoDriver.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tfidfJob, idfOutput);
        Path tfidfOutput = new Path(args[2]);
        FileOutputFormat.setOutputPath(tfidfOutput);
        if (fs.exists(tfidfOutput)) fs.delete(tfidfOutput, true);
        tfidfJob.waitForCompletion(true);
    }
}
