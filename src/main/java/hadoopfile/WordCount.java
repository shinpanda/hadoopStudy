package hadoopfile;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
	
	public static class TokenCounterMapper extends Mapper<Object, Text, Text, IntWritable>{ //중첩 클래스(스테틱 중첩클래스)
		//내용물만 바꿔 재사용
		private Text word = new Text();
		private final IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(Object key, Text value/*라인을 읽어옴*/, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException { // context 객체는 하둡 맵리듀스 시스템과 통신하면서 출력 데이터를 기록하거나, 모니터링에 필요한 상태값이나 메시지를 갱신하는 역할을 함
			// 매핑을 위한 함수
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line); // 공백 단위로 구분
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{ // 집계된 결과
		
		IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// 리듀싱을 하기 위한 함수
			
			int count = 0;
			
			for(IntWritable val:values) {
				count += val.get();
			}
			
			result.set(count);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Count"); //job에 대한 이름 부여
		
		//job에대한 일 부여
		//시작을 할 메인 클래스도 알려줘야함 실행을 담당하는 클래스
		job.setJarByClass(WordCount.class);
		//매퍼 설정 파일을 합치고 무엇을 할 것인지
		job.setMapperClass(TokenCounterMapper.class);
		//리듀서 제공
		job.setReducerClass(IntSumReducer.class);
		
		// key와 value 데이터형식 지정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 인풋 폴더와 아웃풋 폴더 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1); //job 이 끝날 때까지 기다림
	}

}
