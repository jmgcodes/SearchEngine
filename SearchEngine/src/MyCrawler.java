/*
 * Authors: Jwala Mohith Girisha, Rajani R Siddhanamatha, Vijaykumar Koppad
 * Student ID: 12647996, 82721916, 10604535
 * 
 * This class extends the WebCrawler class and overrides its shouldVisit and visit methods.
 * Visited urls and domains are tracked in maps.
 * HTML and text data of crawled pages are stored into files for text processing. 
 *
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;


public class MyCrawler extends WebCrawler {


		private class webPage{ // Custom webPage class
	
			public int pageCount = 0; 
			public String pageFileName = "NA";
			
		}
		
		private static int shouldVisitCount = 0;
		private static int visitedCount = 0;
		
		private static String seedDomain = ".ics.uci.edu"; //Restricting it to this domain
		
		private static Map<String, webPage> urlMap  = new HashMap<String, webPage>(); //Stores all the urls found
		private static Map<String, Integer> domainMap = new TreeMap<String, Integer>(); //Stores the domains visited
		
        private final static Pattern FILTERS = Pattern.compile(".*\\.(bmp|gif|jpe?g|png|tiff?|pdf|ico|xaml|pict|rif|pptx?|ps" +
        														"|mid|mp2|mp3|mp4|wav|wma|au|aiff|flac|ogg|3gp|aac|amr|au|vox" +
        														"|avi|mov|mpe?g|ra?m|m4v|smil|wm?v|swf|aaf|asf|flv|mkv" +
        														"|zip|rar|gz|	7z|aac|ace|alz|apk|arc|arj|dmg|jar|lzip|csv|lha)" +
                												"(\\?.*)?$"); // For url Query parts ( URL?q=... );

        
        // Extract the domain/sub-domain from the url
        
        public String extractDomain(String url){ 
        	
        	String strDomain = "";
        	
        	int index = url.indexOf(seedDomain);
        	
        	strDomain = url.substring(0, index + seedDomain.length());

        	return strDomain;
        	
        }
        
        /**
         * You should implement this function to specify whether
         * the given url should be crawled or not (based on your
         * crawling logic).
         */
        @Override
        public boolean shouldVisit(WebURL url) {
        	
        	
        	boolean duplicatePage = false;
        	
                String href = url.getURL().toLowerCase();
                
               if(!FILTERS.matcher(href).matches() && href.contains(seedDomain)){
                       	   
            	   //Return false if uci.edu is not part of the domain name
            	   if(!(url.getDomain().equals("uci.edu"))){
            		   return false;
            	   }
            	   
            	 
            	   if(href.endsWith("/")){
            		   href = href.substring(0, href.length()-1);
               	  }
             	   
            	  href = href.toLowerCase(); 
               	  int count = 0;
               	  
               	webPage temp;
               	
               	//Update the URLs Map
               	  if(urlMap.containsKey(href)){
               		  temp = urlMap.get(href);
               		  count = temp.pageCount;
               		  duplicatePage = true;
               	  }
               	  else{
               		 temp = new webPage();
               	  }
               	  count++;
               	  temp.pageCount = count;
               	  urlMap.put(href, temp);
               	               	   

               	  shouldVisitCount++;
            	   
            	   // Domain Extraction
            	   
            	   String hrefDomain = extractDomain(href);
            	   
            	   //Update the Domains map
            	   int countDomain = 0;
                	  if(domainMap.containsKey(hrefDomain)){
                		  countDomain = domainMap.get(hrefDomain);
                	  }
                	  countDomain++;
                	  
                	  //Do not count duplicate entries
                	  if(!duplicatePage)
                		  domainMap.put(hrefDomain, countDomain);
                	 
               }

                return !FILTERS.matcher(href).matches() && href.contains(seedDomain);
        }

        /**
         * This function is called when a page is fetched and ready 
         * to be processed by your program.
         */
        @Override
        public void visit(Page page) {     
        	
        	
               String url = page.getWebURL().getURL();

         	   visitedCount++;
         	            	
         	  if(url.endsWith("/")){
         		 url = url.substring(0, url.length()-1);
          	  }
         	  
         	  url = url.toLowerCase();
           
                if (page.getParseData() instanceof HtmlParseData) {
                        HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
                        String text = htmlParseData.getText();
                        String html = htmlParseData.getHtml();

                        try {
                    		
							String fileName = writeToFile(text, html);
							
			               	webPage temp;
			               	
			               		if(urlMap.containsKey(url)){

			               			temp = urlMap.get(url);
			               			temp.pageFileName = fileName;
			               			urlMap.put(url, temp);

			               		}

						} catch (IOException e) {
							e.printStackTrace();
						}
                    	
                }
                if(visitedCount % 100 == 0){
                	try {
                		System.out.println("No of pages visited "+visitedCount );
						writeDomainMap();
					} catch (IOException e) {
						e.printStackTrace();
					}
                }
        }
        
        public String writeToFile(String text, String html) throws IOException{
        	
        	SecureRandom random = new SecureRandom();
        	String fileName =  new BigInteger(130, random).toString(32);
        	  fileName = visitedCount + fileName;
        	
        	  File fileText = new File("/home/vijaykumar/IR_DUMP/" + fileName + ".txt");
//        	  File fileText = new File("/home/jgirisha/Documents/GitHub/IR_DUMP/" + fileName + ".txt");

			if (!fileText.exists()) {
				fileText.createNewFile();
			}

			FileWriter fwSample = new FileWriter(fileText.getAbsoluteFile());
			BufferedWriter bwSample = new BufferedWriter(fwSample);

			bwSample.write(text);

			bwSample.close();
			
			
			File fileHtml = new File("/home/vijaykumar/IR_DUMP_HTML/" + fileName + ".html");
//      	  File fileHtml = new File("/home/jgirisha/Documents/GitHub/IR_DUMP_HTML/" + fileName + ".html");

			if (!fileHtml.exists()) {
				fileHtml.createNewFile();
			}

			FileWriter fwHandle = new FileWriter(fileHtml.getAbsoluteFile());
			BufferedWriter bwHandle = new BufferedWriter(fwHandle);

			bwHandle.write(html);
			
			bwHandle.close();
			return fileName;
        }
        
        public static void writeIndexMap() throws IOException{
        	
        	System.out.println("Should Visit: " + shouldVisitCount + ", Visited count: " + visitedCount);
        	System.out.println("URL Map size: " + urlMap.size() + " ,Domain size: " + domainMap.size());
        	        	
        	File fileIndexMap = new File("./Files/Result/IndexMap.txt");

			if (!fileIndexMap.exists()) {
				fileIndexMap.createNewFile();
			}

			FileWriter fwSample = new FileWriter(fileIndexMap.getAbsoluteFile());
			BufferedWriter bwSample = new BufferedWriter(fwSample);

        	for(Map.Entry<String, webPage> each: urlMap.entrySet()){
        		try {
        				bwSample.write(each.getKey() + " " + each.getValue().pageCount + " " + each.getValue().pageFileName+"\n");
        			
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
        	try {
				bwSample.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

        	urlMap.clear();
        }
        
        public static void writeDomainMap() throws IOException{
        	

        	File fileDomainMap = new File("./Files/Result/Subdomains.txt");

			if (!fileDomainMap.exists()) {
				fileDomainMap.createNewFile();
			}

			FileWriter fwSample = new FileWriter(fileDomainMap.getAbsoluteFile());
			BufferedWriter bwSample = new BufferedWriter(fwSample);

        	for(Map.Entry<String, Integer> each: domainMap.entrySet()){
        		try {
        				bwSample.write(each.getKey() + " " + each.getValue()+"\n");
        			
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
}