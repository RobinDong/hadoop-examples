import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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

  public static class ScoreComparator extends WritableComparator {
    protected ScoreComparator() {
      super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      Text t1 = (Text) w1;
      Text t2 = (Text) w2;
      String[] arr1 = t1.toString().split("\t");
      String[] arr2 = t2.toString().split("\t");

      String name1 = arr1[0];
      String[] item1 = arr1[1].split(",");
      String course1 = item1[0];
      String score1 = item1[1];

      String name2 = arr2[0];
      String[] item2 = arr2[1].split(",");
      String course2 = item2[0];
      String score2 = item2[1];

      int comp = name1.compareTo(name2);

      if (comp == 0) {
        comp = score2.compareTo(score1);
      }

      return comp;
    }
  }

  public static class ScorePartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text key, NullWritable value, int numPartitions) {
      return key.hashCode() % numPartitions;
    }
  }

  public static class SortMapper
    extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      context.write(value, NullWritable.get());
    }
  }

  public static class SortReducer
    extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
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
    job.waitForCompletion(true);

    /* Sorting */
    Job sortJob = Job.getInstance(conf, "School Sort By Score");
    sortJob.setJarByClass(School.class);
    sortJob.setSortComparatorClass(ScoreComparator.class);
    sortJob.setMapperClass(SortMapper.class);
    sortJob.setPartitionerClass(ScorePartitioner.class);
    sortJob.setReducerClass(SortReducer.class);
    sortJob.setOutputKeyClass(Text.class);
    sortJob.setOutputValueClass(NullWritable.class);
    TextInputFormat.addInputPath(sortJob, new Path(args[2]));
    TextOutputFormat.setOutputPath(sortJob, new Path(args[3]));
    System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
  }
}
