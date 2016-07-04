import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class School {

  public static class UserMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    private String uid, name;

    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      String arr[] = line.split(",");
      uid = arr[0].trim();
      name = arr[1].trim();
      context.write(new Text(uid), new Text(name));
    }
  }

  public static class ScoreMapper
      extends Mapper<LongWritable, Text, Text, Text> {

      private String uid, course, score;

      @Override
      public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String arr[] = line.split(",");
        uid = arr[0].trim();
        course = arr[1].trim();
        score = arr[2].trim();

        context.write(new Text(uid), new Text(course + "," + score));
      }
  }

  public static class InnerJoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
      String name = "";
      List<String> courses = new ArrayList<String>();
      List<String> scores = new ArrayList<String>();

      for (Text value : values) {
        String cur = value.toString();
        if (cur.contains(",")) {
          String arr[] = cur.split(",");
          courses.add(arr[0]);
          scores.add(arr[1]);
        } else {
          name = cur;
        }
      }

      if (!name.isEmpty() && !courses.isEmpty() && !scores.isEmpty()) {
        for (int i = 0; i < courses.size(); i++) {
          context.write(new Text(name), new Text(courses.get(i) + "," + scores.get(i)));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "School");
    job.setJarByClass(School.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ScoreMapper.class);
    job.setReducerClass(InnerJoinReducer.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
