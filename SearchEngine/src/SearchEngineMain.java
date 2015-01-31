/*
 * Author: Jwala Mohith Girisha
 * Student ID: 12647996
 * 
 * This is the main class. 
 * 
 * Phase 1:
 * In this class a TextProcessing object is created and used to process a file by tokenizing it and computing the Two Grams and Palindromes in it.
 * 
 */

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
	
		System.out.println("\nPart1: Text Processing\n-----------------------------------------------------------------\n");
		
		//TextProcessing txtProcObj = new TextProcessing();
		//txtProcObj.fnBeginTextProcessing();
		
		System.out.println("\nPart2: Web Crawling\n-----------------------------------------------------------------\n");

		 String crawlStorageFolder = "./Files/Dump";
         int numberOfCrawlers = 1;

         CrawlConfig config = new CrawlConfig();
         config.setCrawlStorageFolder(crawlStorageFolder);
         config.setUserAgentString("testCrawler");

         /*
          * Instantiate the controller for this crawl.
          */
         PageFetcher pageFetcher = new PageFetcher(config);
         RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
         RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
         CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

         
         /*
          * For each crawl, you need to add some seed urls. These are the first
          * URLs that are fetched and then the crawler starts following links
          * which are found in these pages
          */
         controller.addSeed("http://www.ics.uci.edu/~lopes/teaching/cs221W15/index.html#summaries");

         /*
          * Start the crawl. This is a blocking operation, meaning that your code
          * will reach the line after this only when crawling is finished.
          */
         
         System.out.println(config.toString());
         
         controller.start(MyCrawler.class, numberOfCrawlers);  
         
         controller.shutdown();
         System.out.println("End");

	}
	
}