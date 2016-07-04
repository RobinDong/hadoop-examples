import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalcPrime {

  public static final String SPLITS_NUM = "calcprime.splits.num";
  public static final String MAX_RANGE = "calcprime.range.max";

  public static final long DEFAULT_SLITS = 200;
  public static final long DEFAULT_MAX = 10000;

  public static class NumberInputFormat
      extends InputFormat<LongWritable, NullWritable> {

      static class NumberInputSplit extends InputSplit implements Writable {
        long first;
        long count;

        public NumberInputSplit() {}

        public NumberInputSplit(long offset, long length) {
          first = offset;
          count = length;
        }

        public long getLength() throws IOException {
          return 0;
        }

        public String[] getLocations() throws IOException {
          return new String[]{};
        }

        public void readFields(DataInput in) throws IOException {
          first = WritableUtils.readVLong(in);
          count = WritableUtils.readVLong(in);
        }

        public void write(DataOutput out) throws IOException {
          WritableUtils.writeVLong(out, first);
          WritableUtils.writeVLong(out, count);
        }
      }

      static class NumberRecordReader
          extends RecordReader<LongWritable, NullWritable> {
          long first;
          long count;
          long current;

          public NumberRecordReader() {}

          public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
            first = ((NumberInputSplit)split).first;
            count = ((NumberInputSplit)split).count;
            current = first;
          }

          public void close() throws IOException {}

          public LongWritable getCurrentKey() {
            return new LongWritable(current);
          }

          public NullWritable getCurrentValue() {
            return NullWritable.get();
          }

          public float getProgress() throws IOException {
            return current / (float) count;
          }

          public boolean nextKeyValue() {
            if (current >= (count + first)) {
              return false;
            }
            current++;
            return true;
          }
      }

      public RecordReader<LongWritable, NullWritable>
        createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException {
          return new NumberRecordReader();
        }

      public List<InputSplit> getSplits(JobContext job) {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        long splitsNum = getSplitsNum(job);
        long maxRange = getMaxRange(job);
        for (int start = 0; start < splitsNum; ++start) {
          splits.add(new NumberInputSplit(start * maxRange, maxRange));
        }
        return splits;
      }

      public long getSplitsNum(JobContext job) {
        return job.getConfiguration().getLong(SPLITS_NUM, DEFAULT_SLITS);
      }

      public long getMaxRange(JobContext job) {
        return job.getConfiguration().getLong(MAX_RANGE, DEFAULT_MAX);
      }

  }

  public static class NumberMapper
    extends Mapper<LongWritable, NullWritable, LongWritable, NullWritable> {

    public void map(LongWritable key, NullWritable value, Context context)
            throws IOException, InterruptedException {
            long lkey = key.get();
            if (lkey == 1) {
              return;
            }

            if (lkey == 2 || lkey == 3) {
              context.write(key, value);
              return;
            }

            long end = lkey / 2;
            for (int i = 2; i <= end; i++) {
              if (lkey % i == 0) {
                return;
              }
            }
            context.write(key, value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Calc Prime");
    long splitsNum = DEFAULT_SLITS;
    long maxRange = DEFAULT_MAX;
    if (args.length > 1) {
      splitsNum = Long.parseLong(args[1]);
    }
    if (args.length > 2) {
      maxRange = Long.parseLong(args[2]);
    }
    job.getConfiguration().setLong(SPLITS_NUM, splitsNum);
    job.getConfiguration().setLong(MAX_RANGE, maxRange);
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    job.setJarByClass(CalcPrime.class);
    job.setMapperClass(NumberMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setInputFormatClass(NumberInputFormat.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
