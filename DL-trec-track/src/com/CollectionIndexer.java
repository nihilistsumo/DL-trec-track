package com;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class CollectionIndexer {
	
	public void indexBatch(IndexWriter iw, ArrayList<String> collectionCache) {
		StreamSupport.stream(collectionCache.spliterator(), true).forEach(d -> {
			try {
				String docID = d.split("\t")[0];
				String text = d.split("\t")[1];
				Document doc = new Document();
				doc.add(new StringField("docid", docID, Field.Store.YES));
				doc.add(new TextField("doctext", text.toLowerCase(), Field.Store.YES));
				iw.addDocument(doc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		try {
			iw.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void indexCollection(String collectionFile, String indexDirPath, int batchSize) {
		try {
			Directory indexdir = FSDirectory.open((new File(indexDirPath)).toPath());
			IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
			conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
			IndexWriter iw = new IndexWriter(indexdir, conf);
			
			BufferedReader br = new BufferedReader(new FileReader(new File(collectionFile)));
			String l = br.readLine();
			int i = 1;
			while(l != null) {
				ArrayList<String> collectionCache = new ArrayList<String>();
				while(l != null && collectionCache.size() < batchSize) {
					collectionCache.add(l);
					l = br.readLine();
				}
				this.indexBatch(iw, collectionCache);
				System.out.println("Batch commit to index "+i*batchSize+" docs");
				i++;
				l = br.readLine();
			}
			br.close();
			System.out.println("Done indexing");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inputCollectionFile = args[0];
		String outputIndexDirPath = args[1];
		int batchSize = Integer.parseInt(args[2]);
		CollectionIndexer indexer = new CollectionIndexer();
		indexer.indexCollection(inputCollectionFile, outputIndexDirPath, batchSize);
	}

}
