package com;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CollectionReader {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String collectionFilepath = "/media/sumanta/Seagate Backup Plus Drive/dl_track_data/collection.tsv";
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(collectionFilepath)));
			String l = br.readLine();
			int i = 0;
			while(l != null) {
				System.out.println(l);
				l = br.readLine();
				i++;
				if(i==10)
					break;
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
