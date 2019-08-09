package com;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

public class CollectionSearch {
	
	public void writeRetrievalRun(String queriesFile, String collectionIndexDir, int numRet, String outputRunFile, String runid) {
		try {
			FSDirectory fsd = FSDirectory.open((new File(collectionIndexDir)).toPath());
			IndexReader ir = DirectoryReader.open(fsd);
			IndexSearcher is = new IndexSearcher(ir);
			is.setSimilarity(new BM25Similarity());
			
			HashMap<String, String> queries = new HashMap<>();
			BufferedReader br = new BufferedReader(new FileReader(new File(queriesFile)));
			String l = br.readLine();
			while(l != null) {
				queries.put(l.split("\t")[0], l.split("\t")[1]);
				l = br.readLine();
			}
			br.close();
			
			HashMap<String, ArrayList<String>> runLines = new HashMap<>();
			//StreamSupport.stream(queries.keySet().spliterator(), true).forEach(qid -> {
			int k = 1;
			for(String qid:queries.keySet()) {
				try {
					QueryParser qp = new QueryParser("doctext", new StandardAnalyzer());
					String qText = queries.get(qid);
					Query q = qp.parse(QueryParser.escape(qText.toLowerCase()));
					ScoreDoc[] retDocs = is.search(q, numRet).scoreDocs;
					runLines.put(qid, new ArrayList<String>());
					for(int i=0; i<retDocs.length; i++) {
						String docid = is.doc(retDocs[i].doc).get("docid");
						runLines.get(qid).add(docid+" "+(i+1)+" "+retDocs[i].score);
						//bw.write(qid+" Q0 "+docid+" "+(i+1)+" "+retDocs[i].score+" "+runid+"\n");
					}
					System.out.println(k+"/"+queries.keySet().size()+": "+qText);
					k++;
				} catch (ParseException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//});
			
			System.out.println("Now writing run file");
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputRunFile)));
			for(String q:runLines.keySet()) {
				ArrayList<String> retDocLines = runLines.get(q);
				for(String d:retDocLines)
					bw.write(q+" Q0 "+d+" "+runid+"\n");
			}
			bw.close();
			System.out.println("Done");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String queriesFile = args[0];
		String collectionIndexDir = args[1];
		String outRunFile = args[2];
		String runid = args[3];
		int numRet = Integer.parseInt(args[4]);
		
		CollectionSearch cs = new CollectionSearch();
		cs.writeRetrievalRun(queriesFile, collectionIndexDir, numRet, outRunFile, runid);
	}

}
