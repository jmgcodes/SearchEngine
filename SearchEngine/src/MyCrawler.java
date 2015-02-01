import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class MyCrawler extends WebCrawler {

		private static int shouldVisitCount = 0;
		private static int visitedCount = 0;
		private static String seedDomain = ".ics.uci.edu/";
		
		private static Map<String, Integer> urlMap  = new HashMap<String, Integer>();
		private static Map<String, Integer> domainMap = new HashMap<String, Integer>();
		
        private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" 
                                                          + "|png|tiff?|mid|mp2|mp3|mp4"
                                                          + "|wav|avi|mov|mpeg|ram|m4v|pdf|ppt|pptx" 
                                                          + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

        
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
            	   
            	   if(href.endsWith("/")){
            		   href = href.substring(0, href.length()-1);
               	  }
             	   
               	  int count = 0;
               	  if(urlMap.containsKey(href)){
               		  count = urlMap.get(href);
               		  duplicatePage = true;
               	  }
               	  count++;
               	  urlMap.put(href, count);
               	               	   
            	   System.out.println("Should Visit URL: " + href);
            	   shouldVisitCount++;
            	   
            	   
            	   // Domain Extraction
            	   
            	   String hrefDomain = extractDomain(href);
            	   
            	   int countDomain = 0;
                	  if(domainMap.containsKey(hrefDomain)){
                		  countDomain = domainMap.get(hrefDomain);
                	  }
                	  countDomain++;
                	  
                	  if(!duplicatePage)
                		  domainMap.put(hrefDomain, countDomain);
                	  
                	// End
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
         	   /*
         	  if(url.endsWith("/")){
         		 url = url.substring(0, url.length()-1);
         	  }
       	   
         	  int count = 0;
         	  if(urlMap.containsKey(url)){
         		  count = urlMap.get(url);
         	  }
         	  
         	  count++;
         	  
         	  urlMap.put(url, count);
         	   */
         	   visitedCount++;
         	   
         	   System.out.println("Visited URL: " + url);
         	
         	   	
                if (page.getParseData() instanceof HtmlParseData) {
                        HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
                        String text = htmlParseData.getText();
                        String html = htmlParseData.getHtml();
                        List<WebURL> links = htmlParseData.getOutgoingUrls();

                        System.out.println("Text length: " + text.length());
                        System.out.println("Html length: " + html.length());
                        System.out.println("Number of outgoing links: " + links.size());
                        System.out.println("-------------------------------------------------------------------------");
                }
                printCount();
        }
        
        public static void printCount(){
        	
        	System.out.println("Should Visit: " + shouldVisitCount + ", Visited count: " + visitedCount);
        	System.out.println("URL Map size: " + urlMap.size() + " ,Domain size: " + domainMap.size());
        	
        	System.out.println("****************************************");
        	for(Map.Entry<String, Integer> each: urlMap.entrySet()){
        		System.out.println(each.getKey() + ", " + each.getValue());
        	}
        	
        	System.out.println("################################################");
        	/*
        	List<Entry<String, Integer>> tokenPairList = new ArrayList<Entry<String, Integer>>(domainMap.entrySet());
			Collections.sort( tokenPairList, new Comparator<Map.Entry<String, Integer>>()
			{
				public int compare( Map.Entry<String, Integer> mapEntry1, Map.Entry<String, Integer> mapEntry2 )
				{
					return (mapEntry2.getKey()).compareTo( mapEntry1.getKey() );
				}
			} );
				*/		
        	for(Map.Entry<String, Integer> each: domainMap.entrySet()){
        		System.out.println(each.getKey() + ", " + each.getValue());
        	}
        	
        	//urlMap.clear();
        	//domainMap.clear();
        }
}