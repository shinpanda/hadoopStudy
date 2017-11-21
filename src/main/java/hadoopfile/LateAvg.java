package hadoopfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LateAvg {

	public static class BehindTimeMapper extends Mapper<Object, Text, Text, FloatWritable> { // 중첩 클래스(스테틱 중첩클래스)
		// 내용물만 바꿔 재사용
		private Text idWritable = new Text();
		private FloatWritable timeWritable = new FloatWritable();

		@Override
		protected void map(Object key, Text value/* 라인을 읽어옴 */,
				Mapper<Object, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
			// 매핑을 위한 함수
			String line = value.toString();
			String[] tokens = line.split("\t");
			idWritable.set(tokens[0]);
			timeWritable.set(Float.parseFloat(tokens[1]));
			context.write(idWritable, timeWritable);

		}
	}

	public static class TimeAvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> { // 집계된 결과
		private float avgTotal = 0;
		private int countTotal = 0;
		private FloatWritable avgWritable = new FloatWritable();

		@Override
		protected void setup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<FloatWritable> times,
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			countTotal++;
			float total = 0;
			float avg = 0;
			int count = 0;
			for (FloatWritable time : times) {
				total += time.get();
				count++;
			}
			avg = total / count;
			avgTotal += avg;
			avgWritable.set(avg);
			context.write(key, avgWritable);
		}

		@Override
		protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			FloatWritable avgTotalWritable = new FloatWritable(avgTotal / countTotal);
			context.write(new Text("total"), avgTotalWritable);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Late Avg");

		job.setJarByClass(LateAvg.class);
		job.setMapperClass(BehindTimeMapper.class);
		job.setCombinerClass(TimeAvgReducer.class);
		job.setReducerClass(TimeAvgReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
