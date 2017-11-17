package localfile;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

public class Program {

	public static void main(String[] args) throws IOException {
		System.out.println(args[0]);
		System.out.println(args[1]);
/*
		FileInputStream fin = new FileInputStream("res/data.txt");
		Scanner fscan = new Scanner(fin);
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
		fin.close();*/
	}

}

