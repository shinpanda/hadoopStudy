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

	public static class TokenCounterMapper extends Mapper<Object, Text, Text, FloatWritable> { // 중첩 클래스(스테틱 중첩클래스)
		// 내용물만 바꿔 재사용
		private Text word = new Text();
		private final FloatWritable time = new FloatWritable(1);

		@Override
		protected void map(Object key, Text value/* 라인을 읽어옴 */,
				Mapper<Object, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
			// 매핑을 위한 함수
			String line = value.toString();
			String[] split = line.split("\t");
			word.set(split[0]);
			time.set(Float.parseFloat(split[1]));
			context.write(word, time);

		}
	}

	public static class TimeAvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> { // 집계된 결과

		FloatWritable result = new FloatWritable();

		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			int count = 0;
			for(FloatWritable val:values) {
				sum += val.get();
				count ++;
			}
			result.set(sum/count);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Late Avg");

		job.setJarByClass(LateAvg.class);
		job.setMapperClass(TokenCounterMapper.class);
		job.setReducerClass(TimeAvgReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
