/*
 * Authors: Jwala Mohith Girisha, Rajani R Siddhanamatha, Vijaykumar Koppad
 * Student ID: 12647996, 82721916, 10604535
 * 
 * This is the main class. 
 * 
 * Part 1:
 * In this a TextProcessing object is created and used to process a file by tokenizing it and computing the Two Grams and Palindromes in it.
 * 
 * Part 2:
 * This covers the web crawling part of the search engine. The pages obtained by crawling are then processed to retrieve information about high frequency words.
 * 
 */

import java.util.Date;
import java.util.Scanner;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;


public class SearchEngineMain{
	
	public static void main(String[] args) throws Exception{
		
		Scanner in = new Scanner(System.in);
	
		System.out.println("Search Engine Implementation");
	
		/******************* PART 1: Text processing **************************/
		
		//System.out.println("\nText Processing\n-----------------------------------------------------------------\n");
		//TextProcessing txtProcObj = new TextProcessing();
		//txtProcObj.fnBeginTextProcessing();
		
		/***************** PART 2: Web Crawler ******************************/
		/*
		System.out.println("\nWeb Crawling\n-----------------------------------------------------------------\n");

		 String crawlStorageFolder = "./Files/Dump"; 
		 
         int numberOfCrawlers = 10;

         CrawlConfig config = new CrawlConfig();
         config.setCrawlStorageFolder(crawlStorageFolder);
         
         config.setUserAgentString("UCI WebCrawler 10604535 12647996 82721916");
         
         config.setPolitenessDelay(300); 
    
         config.setMaxDepthOfCrawling(80);

         config.setMaxPagesToFetch(5);

         // Instantiate the controller for this crawl.
          
         PageFetcher pageFetcher = new PageFetcher(config);
         RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
         RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
         CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

         
         // For each crawl, you need to add some seed urls. These are the first
         // URLs that are fetched and then the crawler starts following links
         // which are found in these pages
          
         controller.addSeed("http://www.ics.uci.edu/~lopes/index.html");
                  
         
         // Start the crawl. This is a blocking operation, meaning that your code
         // will reach the line after this only when crawling is finished.
          
         
         Date dt1 = new Date();
         controller.start(MyCrawler.class, numberOfCrawlers); 
         Date dt2 = new Date();
         
			long diff = dt2.getTime() - dt1.getTime();
			long diffSeconds = diff / 1000 % 60;
			long diffMinutes = diff / (60 * 1000) % 60;
			long diffHours = diff / (60 * 60 * 1000) % 24;

         System.out.println("Time: " + diffHours + " hr " + diffMinutes + " m " + diffSeconds + " s");

         MyCrawler.writeIndexMap();
         MyCrawler.writeDomainMap();
         
         controller.shutdown();
        
         System.out.println("Web crawling complete... \nText process begins");
		 */
         // Text processing
         
//   TextProcessingWebFiles.textProcessing("./Files/Result/IndexMap.txt");
         
//        System.out.println("End");
		
		
		/***************** PART 3: Indexing ******************************/
		MongoDB Mdb = new MongoDB();
		
//		Mdb.fnCalculateTFIDF();
		
		while(true){
			System.out.println("Enter the string to search");
			
			String search = in.nextLine();
			
			Mdb.fnSearch(search.toLowerCase());
			
			
		
		}
		

	}
	
}