/*
 * Author: Jwala Mohith Girisha
 * Student ID: 12647996
 * 
 * This is the implementation class for TextProcessing. 
 * 
 * Functions:
 * 1. fnBeginTextProcessing: Collects the file name and other commands from the user and invokes appropriate functions.
 * 
 * PartA
 * 2. tokenizeFile: Read the file line by line and converts the words into tokens and stores into a list object.
 * 3. print: Prints the list of tokens to the screen.
 * 
 * PartB
 * 4. computeWordFrequencies: Computes the frequencies of token in the list.
 * 5. printToken: Write the tokens and their frequencies to a file.
 * 
 * PartC
 * 6. computeTwoGramFrequencies: Computes the Two grams from the token list and calculates their frequencies.
 * 7. printTwoGram: Write the two grams and their frequencies to a file.
 * 
 * PartD
 * 8. computePalindromeFrequencies: Computes the Palindromes from the list and calculates their frequencies.
 * 9. isPalindrome: Checks if a string is a palindrome.
 * 10. printPalindrome: Write the Palindromes and their frequencies to a file.
 * 
 * Disclosure:
 * The logic to sort entries of a map object was referred from
 * http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java
 * The code in the above location has been modified to fit the specifications of this problem with clear understanding.
 * 
 */

import java.util.*;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;
 
 
 
public class TextProcessing{

	/***************** START OF fnBeginTextProcessing METHOD *****************************/
	
	public void fnBeginTextProcessing(){
		
		Scanner in = new Scanner(System.in);
		
		List<String> tokenList = new ArrayList();
		Map<String,Integer> tokenPair = new HashMap<String, Integer>();
		Map<String,Integer> twoGramPair = new HashMap<String, Integer>();
		Map<String,Integer> palindromePair = new HashMap<String, Integer>();

		
		/*
		 * Takes file path.
		 * 
		 * Calls functions to tokenize the file, count token frequency, compute two grams and count their frequencies and compute palindromes and count frequency.
		 * 
		 */
		while(true){
			String filePath;
			System.out.println("Enter the file path:");
		
			filePath = in.nextLine();
			File file = new File(filePath);
	
			if(file.exists() && !file.isDirectory()){
				tokenList = tokenizeFile(filePath);
				if(tokenList.size() == 0){
					System.out.println("File empty. Try another file. \n");
					continue;
				}
					
				System.out.println("\n********File tokenized Successfully*********");
				break;
			}
			else{
				System.out.println("File not found. Try again. \n");
			}			
		}
		
		System.out.println(tokenList.size() + " tokens found. Would you like to print the list? [y/n]");
		
		String userip = in.nextLine();
		if(userip.equals("y") || userip.equals("Y"))
			print(tokenList);
			
		
		while(true){
				
			System.out.println("\n----------------------------\nWhat would you like to do? \n1. Compute and print token frequencies.\n2. Compute Two Grams and print their frequency.\n3. Compute palindrome and print their frequency.\n4. All of the above. \n5. Quit\nYour choice?");
			int useripint = in.nextInt();		
		
			if(useripint == 5)
				return;
				
			if(useripint == 1 || useripint == 4){
				
				//PartB
				tokenPair = computeWordFrequencies(tokenList);
				printToken(tokenPair);
			}
			
			if(useripint == 2 || useripint == 4){
				
				//PartC
				twoGramPair = computeTwoGramFrequencies(tokenList);
				printTwoGram(twoGramPair);
			}
			
			if(useripint == 3 || useripint == 4){
				
				//PartD
				palindromePair = computePalindromeFrequencies(tokenList);
				printPalindrome(palindromePair);
			}
		}
	}
	
	/***************** END OF fnBeginTextProcessing METHOD *****************************/



	/***************** START OF tokenizeFile and print METHODS (PART A) *****************************/
	
	//Part A
	/*
	 * Tokenize input file.
	 * Print tokens.
	 *  
	 */
	private List<String> tokenizeFile(String FileName){
		
	
		BufferedReader brSample = null;
		List<String> tokenList = new ArrayList();
		
		try {
 
			String strSample;
			brSample = new BufferedReader(new FileReader(FileName));
 
			while ((strSample = brSample.readLine()) != null) {
					
				for(String words: strSample.toLowerCase().split("[^a-z0-9]+")){
					if(words.length()>0)
						tokenList.add(words);
				}		
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
		finally {
			try {
				if (brSample != null)
					brSample.close();	
			}
			catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	
		return tokenList;
	}
	
	private void print(List<String> tokenList){
		
		System.out.println("Tokens: " + tokenList);
		
	}
	
	/***************** END OF tokenizeFile and print METHODS (PART A) *****************************/

	
	
	/***************** START OF computeWordFrequencies and printToken METHODS (PART B) *****************************/

	//Part B
	/*
	 * Compute token frequencies
	 * Print token frequencies (Writes to file 'Token.txt')
	 * 
	 */
	private Map<String, Integer> computeWordFrequencies(List<String> tokenList){
		
				
		
		Map<String,Integer> tokenPairTemp = new HashMap<String, Integer>();
		
		for (String token : tokenList) {
			Integer count = tokenPairTemp.get(token);
			count = (count == null) ? 1 : ++count;
			tokenPairTemp.put(token, count);
		}
		
		return tokenPairTemp;
	
	}
	
	private void printToken(Map<String, Integer> tokenPair){
		
		try{
			
			File fileTokens = new File("./Files/Result/Tokens.txt");
	 
			if (!fileTokens.exists()) {
				fileTokens.createNewFile();
			}

			FileWriter fwSample = new FileWriter(fileTokens.getAbsoluteFile());
			BufferedWriter bwSample = new BufferedWriter(fwSample);

			
			int tokenCount = 0;
			
			//System.out.println("\n\tFrequency - Token\n\t---------   -----\n");
			
			List<Entry<String, Integer>> tokenPairList = new ArrayList<Entry<String, Integer>>(tokenPair.entrySet());
			Collections.sort( tokenPairList, new Comparator<Map.Entry<String, Integer>>()
			{
				public int compare( Map.Entry<String, Integer> mapEntry1, Map.Entry<String, Integer> mapEntry2 )
				{
					return (mapEntry2.getValue()).compareTo( mapEntry1.getValue() );
				}
			} );
			
			for(Map.Entry<String, Integer> mapEntry:tokenPairList){
				
				tokenCount += mapEntry.getValue();
			  //  System.out.println("\t" + mapEntry.getValue() + "\t" + mapEntry.getKey());
				bwSample.write(mapEntry.getValue() + "\t" + mapEntry.getKey() + "\n");

			}
			
			System.out.println("\n**** Tokens saved into 'Tokens.txt' ****");
			System.out.println("Total number of Tokens: " + tokenCount);
			System.out.println("Total number of Unique Tokens: " + tokenPair.size());

			bwSample.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}

	/***************** END OF computeWordFrequencies and printToken METHODS (PART B) *****************************/

	
	
	/***************** START OF computeTwoGramFrequencies and printTwoGrams METHODS (PART C) *****************************/

	//Part C
	/*
	 * Compute two grams
	 * Print two grams (Write to file 'TwoGrams.txt')
	 * 
	 */
	private Map<String, Integer> computeTwoGramFrequencies(List<String> tokenList){
		
		Map<String,Integer> twoGramPairTemp = new HashMap<String, Integer>();
		
		String previous = "";
		for (String token : tokenList) {
			String tokenTemp = "";
			if(previous == ""){
				previous = token;
				continue;
			}
			else{
				tokenTemp = previous + " " + token;
				previous = token;
			}
			Integer count = twoGramPairTemp.get(tokenTemp);
			count = (count == null) ? 1 : ++count;
			twoGramPairTemp.put(tokenTemp, count);
			
		}
		
		return twoGramPairTemp;
	
	}
	
	private void printTwoGram(Map<String, Integer> twoGramPair){
		
		try{
			File fileTwoGrams = new File("./Files/Result/TwoGrams.txt");
	 
			if (!fileTwoGrams.exists()) {
				fileTwoGrams.createNewFile();
			}

			FileWriter fwSample = new FileWriter(fileTwoGrams.getAbsoluteFile());
			BufferedWriter bwSample = new BufferedWriter(fwSample);

		
		
		int twoGramCount = 0;
		
		//System.out.println("\n\tFrequency - Two Grams\n\t---------   ---------\n");
		
        List<Entry<String, Integer>> twoGramPairList = new ArrayList<Entry<String, Integer>>(twoGramPair.entrySet());
        Collections.sort( twoGramPairList, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare( Map.Entry<String, Integer> mapEntry1, Map.Entry<String, Integer> mapEntry2 )
            {
                return (mapEntry2.getValue()).compareTo( mapEntry1.getValue() );
            }
        } );
        
        for(Map.Entry<String, Integer> mapEntry: twoGramPairList){
			
			twoGramCount += mapEntry.getValue();
            //System.out.println("\t" + mapEntry.getValue() + "\t" + mapEntry.getKey());
			bwSample.write(mapEntry.getValue() + "\t" + mapEntry.getKey() + "\n");

        }
        
        System.out.println("\n**** Two Grams saved into 'TwoGrams.txt' ****");
		System.out.println("Total number of Two Grams: " + twoGramCount);
		System.out.println("Total number of Unique Two Grams: " + twoGramPair.size());
		
		bwSample.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
						
	}
	
	/***************** END OF computeTwoGramFrequencies and printTwoGrams METHODS (PART C) *****************************/

	
	
	/***************** START OF computePalindromeFrequencies and printPalindromes METHODS (PART D) *****************************/

	//Part D
	/*
	 * Compute palindromes
	 * Print palindromes (Write to file 'Palindromes.txt')
	 * 
	 */
	private Map<String, Integer> computePalindromeFrequencies(List<String> tokenList){

		Map<String,Integer> palindromePairTemp = new HashMap<String, Integer>();
		
		int indexToken = 0;	
		int indexNotIncl = -1;	
		
		for (;indexToken<tokenList.size();) {
						
			boolean palindrome = false;
				
			int index = indexToken;
			int indexPal = -1;
			
			int strlen = 0;
			String strTemp = "";
			String strPalindrome = "";
			
			while(strlen<=20 && index < tokenList.size()){
				
				strTemp += tokenList.get(index);
				
				palindrome = isPalindrome(strTemp);
			
				if(palindrome){	
					
					strPalindrome = strTemp;
					indexPal = index;
					
				}
				
				strlen = strTemp.length();
				index++;
			}
			
			if(strPalindrome.length() > 1 && indexPal > indexNotIncl){
				
				Integer count = palindromePairTemp.get(strPalindrome);
				count = (count == null) ? 1 : ++count;
				palindromePairTemp.put(strPalindrome, count);
				
				indexNotIncl = indexPal;
				
			}

			
			indexToken++;
		}
		
		return palindromePairTemp;
	
	}
	
	private boolean isPalindrome(String str){
		
		
		int len = str.length();
			
			for(int i=0, j=len-1; i<j; i++, j--){
				if(str.charAt(i)==str.charAt(j))
					continue;
				else{
					return false;
				}
			}
			
			return true;
	}
	
	private void printPalindrome(Map<String, Integer> palindromePair){
		
		try{
			File filePalindrome = new File("./Files/Result/Palindromes.txt");
	 
			if (!filePalindrome.exists()) {
				filePalindrome.createNewFile();
			}

			FileWriter fwSample = new FileWriter(filePalindrome.getAbsoluteFile());
			BufferedWriter bwSample = new BufferedWriter(fwSample);
		
		int palindromeCount = 0;
				
		//System.out.println("\n\tFrequency - Palindromes\n\t---------   -----------\n");
		
        List<Entry<String, Integer>> palindromePairList = new ArrayList<Entry<String, Integer>>(palindromePair.entrySet());
        Collections.sort( palindromePairList, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare( Map.Entry<String, Integer> mapEntry1, Map.Entry<String, Integer> mapEntry2 )
            {
                return (mapEntry2.getValue()).compareTo( mapEntry1.getValue() );
            }
        } );
        
        for(Map.Entry<String, Integer> mapEntry: palindromePairList){
			
			palindromeCount += mapEntry.getValue();
           // System.out.println("\t" + mapEntry.getValue() + "\t\t" + mapEntry.getKey());
   			bwSample.write(mapEntry.getValue() + "\t" + mapEntry.getKey() + "\n");

        }
        
        System.out.println("\n**** Palindromes saved into 'Palindromes.txt' ****");
        System.out.println("Total number of Palindromes: " + palindromeCount);
		System.out.println("Total number of Unique Palindromes: " + palindromePair.size());

		bwSample.close();
		

		} catch (IOException e) {
			e.printStackTrace();
		}				
	}
	
	/***************** END OF computePalindromeFrequencies and printPalindromes METHODS (PART D) *****************************/

	
}