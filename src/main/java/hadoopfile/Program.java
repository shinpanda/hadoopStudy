package hadoopfile;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Program {
//#  hadoop jar
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();// core-site접근하여 서버 정보 받아옴
		
		FileSystem hdfs = FileSystem.get(conf); // 파일시스템 접근
		
		//Path filePath = new Path("/input/data.txt");
		Path filePath = new Path(args[0]);
		
		if(!hdfs.exists(filePath)) {
			System.err.println("입력오류 : "+filePath+"가 존재하지 않습니다");
			System.exit(2);
		}
		
		//FileInputStream fis = new FileInputStream("res/data.txt");
		FSDataInputStream fis = hdfs.open(filePath);
		Scanner fscan = new Scanner(fis);
		//while(scan.hasNext())
		//System.out.println(scan.nextLine());
		float sum = 0;
		int count = 0;
		while(fscan.hasNext()) {
			String line = fscan.nextLine();
			String[] tokens = line.split("\t");
			sum += Float.parseFloat(tokens[1]);
			count++;
		}
		System.out.println(sum);
		System.out.println(sum/count);
		fscan.close();
		fis.close();
	}

}
