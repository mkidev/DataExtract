package edu.mki.bachelor.dataimport;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class FileCodexConvert {
	public static void main(String[] args) {
		convertFromTo("wordList.txt", "wordList_modded.txt",
				Charsets.ISO_8859_1, Charsets.UTF_8);
	}

	public static void convertFromTo(String inFileName, String outFileName,
			Charset from, Charset to) {
		System.out.println("Start");
		File file = new File(inFileName);
		try {
			String a = Files.toString(file, from);
			System.out.println(a);
			File outFile = new File(outFileName);
			Files.write(a, outFile, to);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Ende");
	}
}
