/*
 * Authors: Jwala Mohith Girisha, Rajani R Siddhanamatha, Vijaykumar Koppad
 * Student ID: 12647996, 82721916, 10604535
 * 
 * The stored text files are tokenized and high frequency words and 2-grams are calculated.
 *
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

class posObj{
	 private String docID = "";
	 private int freq =0; // frequency of the word per document
	 private List<Integer> position = new ArrayList<Integer>();
	 
	 
	 public void setDocID(String docID){
		 this.docID = docID;
	 }
	 
	 public String getDocID(){
		 return this.docID;
	 }

	 public void incFreq(){
		 this.freq++;
	 }
	 
	 public void add(int num){
		 this.position.add(num);
	 }
	 
	 public List<Integer> getPos(){
		 return this.position;
	 }
	 
	 public int getFreq(){
		 return this.freq;
	 }
}

 


public class TextProcessingWebFiles{
	
	 
     private static int maxCnt = 0;	
	 private static String maxLink = "";
	 private static int docId = 0;
	 private static Map<String, String>  webLinkID = new HashMap<String,String>();
	 private static Map<String,  List<posObj> >  wordMap = new HashMap<String, List<posObj> >();
	 
	 // private static Map<String, ListdocObj>  wordList = new HashMap<String,Integer>();
	  private static Map<String, Integer>  wordList = new HashMap<String,Integer>();
     //  private static Map<String, Integer>  twoGramList = new HashMap<String,Integer>();
	 
     private static List<String> stopWords = new ArrayList<String>(
    		 Arrays.asList( "a","about","above","after","again","against","all","am","an","and","any","are","aren't",
    				 "as","at","be","because","been","before","being","below","between","both","but","by","can't","cannot",
    				 "could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for",
    				 "from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here",
    				 "here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is",
    				 "isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off",
    				 "on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shan't","she","she'd",
    				 "she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them",
    				 "themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through",
    				 "to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what",
    				 "what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would",
    				 "wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves")
    		 );
	 
     
     
	 public static void textProcessing (String filePath) throws IOException {
			
		FileReader inputFile = new FileReader(filePath);
			
		BufferedReader bufferread = new BufferedReader(inputFile);
		
		String eachline;
		
		MongoDB Mdb = new MongoDB();
		
		
		try
		{
			while((eachline = bufferread.readLine() ) != null){
			eachline = eachline.trim();
			if(!( eachline.isEmpty())){
				String[] attr = eachline.split(" ");
				int fileIndex = attr.length - 1;
				String textFileName = attr[fileIndex];
				String webLink  = attr[0];
				if(!textFileName.equals("NA")){
					fnCountWords(textFileName, webLink);
				}
				if(docId % 100 == 0){
					System.out.printf("Processing docs %d and %d ... \n", docId, docId+100);
				}
				
				if(docId%10000 == 0){	
					Mdb.fnMongo(wordMap);
					wordMap.clear();
					Mdb.fnWriteDocMap(webLinkID);
					webLinkID.clear();
				}
			}
		  }
			Mdb.fnMongo(wordMap);
			wordMap.clear();
			
			Mdb.fnWriteDocMap(webLinkID);
			webLinkID.clear();

		}

		catch(IOException e)
		{
		     System.out.println(e);	
		}
		
		finally
		{
			try {
				
				if(bufferread != null)
					bufferread.close();
				
			}
			catch(IOException e){
				System.out.println(e);
			}
		}
		
		
		//printTopCommonWords();
		/*printTopTwoGrams();
		printMaxWeblink();*/
		

	}
	 
	 public static void fnUpdateTitle(String filePath) throws FileNotFoundException, UnknownHostException{
		 
			FileReader inputFile = new FileReader(filePath);
			
			BufferedReader bufferread = new BufferedReader(inputFile);
			
			String eachline;
						
			try
			{
				while((eachline = bufferread.readLine() ) != null){
				eachline = eachline.trim();
				if(!( eachline.isEmpty())){
					String[] attr = eachline.split(" ");
					int fileIndex = attr.length - 1;
					String textFileName = attr[fileIndex];
					String webLink  = attr[0];
					if(!textFileName.equals("NA")){
						fnParseHTML(textFileName, webLink);
					}
				}
			  }
			}

			catch(IOException e)
			{
			     System.out.println(e);	
			}
			
			finally
			{
				try {
					
					if(bufferread != null)
						bufferread.close();
					
				}
				catch(IOException e){
					System.out.println(e);
				}
			}
	 

	 }
	 
	 public static void fnParseHTML(String fileName, String webLink) throws IOException{
		 
		 
			File fileText = new File("/home/jgirisha/Documents/GitHub/IR_DUMP_HTML/" + fileName + ".html");

			if (!fileText.exists()) {
				return;
			}
         		
			FileInputStream fis = new FileInputStream(fileText);
			byte[] data = new byte[(int) fileText.length()];
			fis.read(data);
			fis.close();

			String HTMLSTring = new String(data, "UTF-8");

			Document html = Jsoup.parse(HTMLSTring);
	        String title = html.title();
	        String h1 = html.body().getElementsByTag("h1").text();
	 
	        System.out.println("Input HTML String to JSoup :" + HTMLSTring);
	        System.out.println("After parsing, Title : " + title);
	        System.out.println("Afte parsing, Heading : " + h1);
		 
	 }
	 
	 public static void fnCountWords(String fileName, String webLink) throws FileNotFoundException{
		 
//			FileReader inputFile = new FileReader("/home/vijaykumar/IR_DUMP/" + fileName );
//			FileReader inputFile = new FileReader("/home/jgirisha/Documents/GitHub/IR_DUMP/" + fileName + ".txt");
			FileReader inputFile = new FileReader("/home/jgirisha/Documents/GitHub/IR_DUMP/" + fileName );
			//FileReader inputFile = new FileReader("/home/rajanisr/IR_DUMP/" + fileName + ".txt");

//      	  File fileText = new File("/home/vijaykumar/IR_DUMP/" + fileName);
      	  //File fileText = new File("/home/jgirisha/Documents/GitHub/IR_DUMP/" + fileName + ".txt");
	      	  File fileText = new File("/home/jgirisha/Documents/GitHub/IR_DUMP/" + fileName);
	      	  //File fileText = new File("/home/rajanisr/IR_DUMP/" + fileName + ".txt");

			if (!fileText.exists()) {
				return;
			}
            
			docId++;
			String strDocID = "" + docId;
			webLinkID.put(strDocID, webLink);

			
			BufferedReader bufferread = new BufferedReader(inputFile);
			String eachline;
			int wordCnt = 0;
			int wrdPos = 0; 
			
			try
			{
				while((eachline = bufferread.readLine() ) != null){
				eachline = eachline.trim();
				if(!( eachline.isEmpty())){
					String[] attr = eachline.toLowerCase().split("[^a-zA-Z0-9]+");
					
					String Previous = "";
					for(String word: attr){
						
						
						
						if(stopWords.contains(word) || word.length() < 2)
							continue;
						
						else{
							
								Integer count = wordList.get(word);
								count = (count == null) ? 1 : ++count;
								wordList.put(word, count);

							
							wrdPos++;
							posObj temppos;
							List<posObj> tempdocList;
							boolean flag = true;
							
							if(wordMap.containsKey(word)){
								tempdocList = wordMap.get(word);
								
								int size = tempdocList.size();
								temppos = (posObj)tempdocList.get(size-1);
								
								if(temppos.getDocID() == strDocID){
									flag = false;
								}
								else{
									temppos = new posObj();
									temppos.setDocID(strDocID);
								}

							}
							else{
								tempdocList = new ArrayList<posObj>();
								temppos = new posObj();
								temppos.setDocID(strDocID);

							} 
							
							
							temppos.incFreq();
							temppos.add(wrdPos);

							if(flag)
								tempdocList.add(temppos);
							
							wordMap.put(word, tempdocList);
							

						}
					}
				}
			  }
			}

			catch(IOException e)
			{
			     System.out.println(e);	
			}
			
			finally
			{
				if(wordCnt > maxCnt){
					maxCnt	= wordCnt;
					maxLink = webLink;					
				}
				
				
				try {
					
					if(bufferread != null)
						bufferread.close();
					
				}
				catch(IOException e){
					System.out.println(e);
				}
				
			}
	 }
	 
	 /*
	 public static void printMaxWeblink() throws IOException{
		 File fileWeblink = new File("./Files/Result/maxWeblink.txt");
	     	
		 if (!fileWeblink.exists()) {
			 fileWeblink.createNewFile();
		 }

		 FileWriter fwSample = new FileWriter(fileWeblink.getAbsoluteFile());
		 BufferedWriter bwSample = new BufferedWriter(fwSample);
	     	
	     try {
	    	bwSample.write(maxLink+ " " + maxCnt +"\n");
	     				
	     }catch (IOException e) {
		     e.printStackTrace();
		 }
	    
	     	try {
					bwSample.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

		 } 
	 
	 
	 public static void printTopCommonWords() throws IOException{
		 
     	File fileDomainMap = new File("./Files/Result/Words.txt");
     	int count = 5000;
     	
			if (!fileDomainMap.exists()) {
				fileDomainMap.createNewFile();
			}

			FileWriter fwSample = new FileWriter(fileDomainMap.getAbsoluteFile());
			BufferedWriter bwSample = new BufferedWriter(fwSample);

			List<Entry<String, Integer>> wordPairList = new ArrayList<Entry<String, Integer>>(wordList.entrySet());
			Collections.sort( wordPairList, new Comparator<Map.Entry<String, Integer>>()
			{
				public int compare( Map.Entry<String, Integer> mapEntry1, Map.Entry<String, Integer> mapEntry2 )
				{
					return (mapEntry2.getValue()).compareTo( mapEntry1.getValue() );
				}
			} );

     	for(Map.Entry<String, Integer> each:wordPairList){
     		try {
     				if(count>0){
     				bwSample.write(each.getValue() + " " + each.getKey() +"\n");
     				count--;
     				}
     				else{
     					break;
     				}
				} catch (IOException e) {
					e.printStackTrace();
				}
     	}
     	try {
				bwSample.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

	 }
	 
	 public static void printTopTwoGrams() throws IOException{
		 
	     	File fileDomainMap = new File("./Files/Result/TwoGrams.txt");
	     	int count = 20;
	     	
				if (!fileDomainMap.exists()) {
					fileDomainMap.createNewFile();
				}

				FileWriter fwSample = new FileWriter(fileDomainMap.getAbsoluteFile());
				BufferedWriter bwSample = new BufferedWriter(fwSample);
				
				List<Entry<String, Integer>> wordPairList = new ArrayList<Entry<String, Integer>>(twoGramList.entrySet());
				Collections.sort( wordPairList, new Comparator<Map.Entry<String, Integer>>()
				{
					public int compare( Map.Entry<String, Integer> mapEntry1, Map.Entry<String, Integer> mapEntry2 )
					{
						return (mapEntry2.getValue()).compareTo( mapEntry1.getValue() );
					}
				} );

	     	for(Map.Entry<String, Integer> each: wordPairList){
	     		try {
	     				if(count>0){
	     				bwSample.write(each.getValue() + " " + each.getKey() +"\n");
	     				count--;
	     				}
	     				else{
	     					break;
	     				}
					} catch (IOException e) {
						e.printStackTrace();
					}
	     	}
	     	try {
					bwSample.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		 }*/
}