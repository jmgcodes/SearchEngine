import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.*;




public class MongoDB{
	
	private class RankObject{
		
		int qrWC;
		double tfidf_score;
		Set<Integer> posSet;
		double cos_score;
		double mag_doc;
		
		double page_rank;
		
		RankObject(){
			this.qrWC = 0;
			this.tfidf_score = 0.0;
			this.posSet = new TreeSet<Integer>();
			this.cos_score = 0.0;
			this.mag_doc = 0.0;
			
			this.page_rank = 0.0;
		}
		
	}
	
	
	static MongoClient mongoClient;
	static DB db;
	static DBCollection coll;
	static DBCollection coll1;
	
	MongoDB() throws UnknownHostException {

		mongoClient = new MongoClient( "localhost" , 27017 );
		db = mongoClient.getDB("InvertedIndex");
		coll = db.getCollection("InvIndex");
	    coll1 = db.getCollection("DocMap");
	}
	
   public  void fnMongo(Map<String,  List<posObj>> wordMap, Map<String,List<Integer>>wordTitleMap){
      try{   
    	  
          int count = 0;
          
          List<posObj> docList = new ArrayList<posObj>();
          List<Integer> docTitleList = new ArrayList<Integer>();

          System.out.println("Mapsize: " + wordMap.size());

          Iterator iterMap = wordMap.keySet().iterator();
          while(iterMap.hasNext()){
        	  
        	  if(count%1000==0)
        		  System.out.println("Storing word " + count + " to " + (count+1000));
        		  
        	  count++;

        	  String word = (String)iterMap.next();
        	  docList = wordMap.get(word);
        	  
        	  docTitleList.clear();
        	  if(wordTitleMap.containsKey(word))
        		  docTitleList = wordTitleMap.get(word);
        	  
        	  
              BasicDBObject query = new BasicDBObject();
              query.put("word", word);
              
              DBCursor cursor = coll.find(query);
              if (cursor.hasNext()) { 
            	  
                  DBObject obj = (DBObject) cursor.next();
                 
                  List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
                  List<Integer> tempTitle = (List<Integer>)obj.get("title");
                  for(posObj tempposobj: docList){
                   	  temp.add(new BasicDBObject("id",tempposobj.getDocID()).append("pos", tempposobj.getPos()).append("frequency", tempposobj.getFreq()));
                  }
                  
                  tempTitle.addAll(docTitleList);
                  
                 obj.put("doc", temp);
                 obj.put("title", tempTitle);
                 coll.save(obj); 
                 
              }
              else{
            	                 
            	  List<BasicDBObject> listObj = new ArrayList<BasicDBObject>();
            	  
                  for(posObj tempposobj: docList){
                	  listObj.add(new BasicDBObject("id",tempposobj.getDocID()).append("pos", tempposobj.getPos()).append("frequency", tempposobj.getFreq()));
                  }
                  
                  coll.insert(new BasicDBObject("doc",listObj).
                		  append("word",word).append("title", docTitleList));

            	  
              }

        	  
          }
          
      }catch(Exception e){
	     System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	  }
      
   }
   
   public void fnFind(String search) throws UnknownHostException{
	   
	    
        BasicDBObject query = new BasicDBObject();
        query.put("word", search);
        
        DBCursor cursor = coll.find(query);
        if (cursor.hasNext()) { 
      	  
            DBObject obj = (DBObject) cursor.next();
                      
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

            for(BasicDBObject tempObj: temp){
            	
                BasicDBObject queryUrl = new BasicDBObject();
                queryUrl.put("docID", tempObj.get("id"));
                DBCursor cursorUrl = coll1.find(queryUrl);
                
                if (cursorUrl.hasNext()) { 
              	  
                    DBObject objUrl = (DBObject) cursorUrl.next();
                              
                	System.out.println("[" +tempObj.get("frequency")+"] " + objUrl.get("url"));
                }
                
            	
            }
     	 
        }
        else{
        	System.out.println("Sorry! Word not found");

        }
        
   }
   
   
   public void fnSearch(String search) throws UnknownHostException{
	   	   
       int N = 85000;
	   
	   System.out.println("1" + new Date());

       
	   String[] searchArr = search.split(" ");
	   int querySize = searchArr.length;
	   double mag_query = 0.0;
	   
	   Map<String, Double> queryMap = new HashMap<String, Double>();
	   for(String word: searchArr){
		   
		   double queryWC = 0.0;
		   if(queryMap.containsKey(word)){
			   
			   queryWC = queryMap.get(word);
		   }
		   queryWC += 1.0;
		   queryMap.put(word, queryWC);
		   
	   }
	   
	   System.out.println("2" + new Date());

	   int queryWordPros = 0;
	   Map<String,RankObject> AndOrTempMap = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMap = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrTitleMap = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrTitleFinalMap = new HashMap<String, RankObject>();

	   for(String word: searchArr){
		   
		   queryWordPros++;
		   BasicDBObject query = new BasicDBObject();
	       query.put("word", word);
	       DBCursor cursor = coll.find(query);
	       
	       if (cursor.hasNext()) {   
	           DBObject obj = (DBObject) cursor.next();	                     
	           List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
	           
	           double queryTF = queryMap.get(word);
	           queryTF = (1+Math.log10(queryTF))*Math.log10((double)N/temp.size());
	           queryMap.put(word, queryTF);
	           
	           mag_query = Math.sqrt((Math.pow(mag_query, 2)+Math.pow(queryTF, 2)));
	           
	           List<Integer> tempTitle = (List<Integer>) obj.get("title");
	           Set<Integer> tempTitleSet = new HashSet();
	           tempTitleSet.addAll(tempTitle);
	           tempTitle.clear();
	           tempTitle.addAll(tempTitleSet);
	           
	           for(int docIDTitle: tempTitle){
	        	   
	   				String strDocIDTitle = "" + docIDTitle;

	   				RankObject objTitle;
	        	   
	        	   if(AndOrTitleMap.containsKey(strDocIDTitle)){
	        		   
	        		   objTitle = AndOrTitleMap.get(strDocIDTitle);
	        		   objTitle.qrWC++;
	        		   
	        	   }
	        	   else{
	        		   objTitle = new RankObject();
	        		   objTitle.qrWC++;
	        		   
	        	   }
	        	   
	        	   AndOrTitleMap.put(strDocIDTitle,objTitle);
	        	  
	        	   

	           }
	           
	           
	           for(BasicDBObject tempObj: temp){
	        	   int countrobj = 1;
	        	   
	        	   String docid = tempObj.get("id").toString();
	        	   double tfidfrobj = (double)tempObj.get("tfidf");
	        	   	        	   
	        	   RankObject robj;
	        	   if(AndOrTempMap.containsKey(docid)){
	        		   robj = AndOrTempMap.get(docid);
	        		   countrobj = robj.qrWC;
	        		   robj.qrWC = countrobj+1;
	        		   
	        		   robj.tfidf_score += tfidfrobj;
	        		   
	        	   }
	        	   else{
	        		   robj = new RankObject();
	        		   robj.qrWC = countrobj;
	        		   robj.tfidf_score = tfidfrobj;
	        		   
	        		   
	        	   }
	        	   
	        	   robj.cos_score += (tfidfrobj*queryTF);
	        	   robj.mag_doc = Math.sqrt((Math.pow(robj.mag_doc, 2)+Math.pow(tfidfrobj, 2)));
	        	   
        		   robj.posSet.addAll((List<Integer>)tempObj.get("pos"));
        		   
        		   if(queryWordPros == querySize){
        			   
        			   if(robj.qrWC == querySize){
        				   
        				   //Check
        					Iterator treIterator = robj.posSet.iterator();
        					int prev = 0;
        					boolean flag = false;
        					while(treIterator.hasNext()){
        						int curr = (int)treIterator.next();
        						if(prev>0){
        							if(prev == curr-1){
        								flag = true;
        								break;
        							}
        						}
    							prev = curr;
        					}
        					if(!flag)
             				   robj.posSet.clear();
        			   }
        			   else
        				   robj.posSet.clear();
        			   
        		   }
        		   
        		   AndOrTempMap.put(docid, robj);
	        	   	        	   
	           }
	       } 
	   }
	   
	   System.out.println("3" + new Date());

	   queryMap.clear();
	   /*
       Iterator itrTitle = AndOrTitleMap.keySet().iterator();
       while(itrTitle.hasNext()){
    	   
    	  
    	   
    	   String keyTitle = (String)itrTitle.next();
    	   
    	   if(AndOrTitleMap.get(keyTitle).qrWC == querySize){
    		   AndOrTitleFinalMap.put(keyTitle, AndOrTitleMap.get(keyTitle));
    	   }
    	   
       }
	*/
       //AndOrTitleMap.clear();
       AndOrTitleFinalMap.clear();
       
       Iterator itrMap = AndOrTempMap.keySet().iterator();
       while(itrMap.hasNext()){
    	   
    	   String keyTemp = (String)itrMap.next();
    	   RankObject robjTemp = AndOrTempMap.get(keyTemp);
    	   double cos = robjTemp.cos_score/(robjTemp.mag_doc*mag_query);
    	   
    	   if(AndOrTitleMap.containsKey(keyTemp))
    		   robjTemp.page_rank = 10*(cos)+10*AndOrTitleMap.get(keyTemp).qrWC + robjTemp.tfidf_score;
    	   else
    		   robjTemp.page_rank = 10*(cos) + robjTemp.tfidf_score;

    	   AndOrTempMap.put(keyTemp, robjTemp);
    	   //if(!AndOrTitleFinalMap.containsKey(keyTemp)){
    		   AndOrMap.put(keyTemp, AndOrTempMap.get(keyTemp));
    	   //}
    	   
       }

       AndOrTempMap.clear();
	   
	   if(AndOrMap.size() == 0 && AndOrTitleFinalMap.size()==0){
		   
		   System.out.println("Not found\n");
		   return;
		   
	   }
		   
	   	List<Entry<String, RankObject>> tokenPairList = new ArrayList<Entry<String, RankObject>>();

	   	if(AndOrTitleFinalMap.size() > 0){
	   		
			List<Entry<String, RankObject>> tokenPairTitleList = new ArrayList<Entry<String, RankObject>>(AndOrTitleFinalMap.entrySet());
			Collections.sort( tokenPairTitleList, new Comparator<Map.Entry<String, RankObject>>()
			{
				public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
				{
					return (new Integer(mapEntry2.getValue().qrWC)).compareTo( new Integer(mapEntry1.getValue().qrWC));
				}
			
			} );

		   	//tokenPairList.addAll(tokenPairTitleList);

	   	}

	   	
	   	if(AndOrMap.size()>0){
		  
	   Map<String,RankObject> AndOrMapTopOcc = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMapTop = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMapHigh = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMapLow = new HashMap<String, RankObject>();
 
	   	List<Entry<String, RankObject>> AndOrList = new ArrayList<Entry<String, RankObject>>(AndOrMap.entrySet());
	   //	List<Entry<String, RankObject>> AndOrListLow = new ArrayList<Entry<String, RankObject>>();

		for(Map.Entry<String, RankObject> mapEntry:AndOrList){

			boolean flag_title = false;
			if(AndOrTitleMap.containsKey(mapEntry.getKey())){
				//if(AndOrTitleMap.get(mapEntry.getKey()).qrWC > querySize/2)
					flag_title = true;
			}
			
			if((mapEntry.getValue().qrWC == querySize && mapEntry.getValue().posSet.size()>0) || flag_title){
				AndOrMapTopOcc.put(mapEntry.getKey(), mapEntry.getValue());
			}
			else if(mapEntry.getValue().qrWC == querySize){
				AndOrMapTop.put(mapEntry.getKey(), mapEntry.getValue());
			}
			else if(mapEntry.getValue().qrWC > ((querySize>3)?(querySize-2):1)){
				AndOrMapHigh.put(mapEntry.getKey(), mapEntry.getValue());
			}
			else{
				AndOrMapLow.put(mapEntry.getKey(), mapEntry.getValue());
			}
	
		}
	   	
		   	List<Entry<String, RankObject>> tokenPairTOList = new ArrayList<Entry<String, RankObject>>();
			   //	tokenPairTList = fnPositionSort(AndOrMapTop,searchArr);
			   	tokenPairTOList = fnSort(AndOrMapTopOcc);

	   	List<Entry<String, RankObject>> tokenPairTList = new ArrayList<Entry<String, RankObject>>();
	   //	tokenPairTList = fnPositionSort(AndOrMapTop,searchArr);
	   	tokenPairTList = fnSort(AndOrMapTop);


	   	List<Entry<String, RankObject>> tokenPairHList = new ArrayList<Entry<String, RankObject>>();
	   	tokenPairHList = fnSort(AndOrMapHigh);
	   	

		List<Entry<String, RankObject>> tokenPairLList = new ArrayList<Entry<String, RankObject>>();
		tokenPairLList = fnSort(AndOrMapLow);
		
	   	
	   	tokenPairList.addAll(tokenPairTOList);
	   	tokenPairList.addAll(tokenPairTList);
	   	tokenPairList.addAll(tokenPairHList);
	   	tokenPairList.addAll(tokenPairLList);
	   }
	   	
		
		int size = tokenPairList.size();
		
		Scanner in = new Scanner(System.in);
		String input = "n";
		
		int current = 0;
		int limit = 0;
		int page = 1;
		
		while(input.matches("n")){
		
			System.out.println("\n-----------------------------------------------------------\n" + search + " [ Page " + page + " ]\n\n" );
			
				if(current + 20 > size)
					limit = size;
				else
					limit = current + 20;
			
				
		for(int i = current; i< limit; i++, current++){
			Map.Entry<String, RankObject> mapEntry = tokenPairList.get(i);
//			System.out.println(mapEntry.getValue());
			 BasicDBObject queryUrl = new BasicDBObject();
			 queryUrl.put("docID", mapEntry.getKey());
			 DBCursor cursorUrl = coll1.find(queryUrl);
           
			 if (cursorUrl.hasNext()) { 
				 
				DBObject objUrl = (DBObject) cursorUrl.next();
                      
				if(AndOrTitleMap.containsKey(mapEntry.getKey())){
	           		System.out.println(mapEntry.getValue().qrWC + " " + AndOrTitleMap.get(mapEntry.getKey()).qrWC + " " + mapEntry.getValue().page_rank +" "+ + mapEntry.getValue().cos_score/(mapEntry.getValue().mag_doc*mag_query) +" "+ + mapEntry.getValue().tfidf_score +" "+ objUrl.get("url"));		
				}
				else
	           		System.out.println(mapEntry.getValue().qrWC + " " + "NA" + " " + mapEntry.getValue().page_rank +" "+ + mapEntry.getValue().cos_score/(mapEntry.getValue().mag_doc*mag_query) +" "+ + mapEntry.getValue().tfidf_score +" "+ objUrl.get("url"));		

				
          		
           }
		}
		
		if(current>=size)
			break;
		
		System.out.println("\n\n'n' for page " + ++page);
		input = in.nextLine();
		

		}
		
		System.out.println("\n\n");
		       
  }
   
   public List<Entry<String, RankObject>> fnSort(Map<String,RankObject> mapToSort){
	   
		List<Entry<String, RankObject>> tokenPairList = new ArrayList<Entry<String, RankObject>>(mapToSort.entrySet());
		Collections.sort( tokenPairList, new Comparator<Map.Entry<String, RankObject>>()
		{
			public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
			{
			    Double value2 = Double.valueOf(mapEntry2.getValue().page_rank);    
			    Double value1 = Double.valueOf(mapEntry1.getValue().page_rank);
			    
				return (value2).compareTo( value1 );
			}
		
		} );

		return tokenPairList;
	   
   }
   
   
   public List<Entry<String, RankObject>> fnPositionSort(Map<String,RankObject> mapToSort, String[] searchArr){

		List<Entry<String, RankObject>> tokenPairList = new ArrayList<Entry<String, RankObject>>(mapToSort.entrySet());
		List<Entry<String, RankObject>> tokenPairHList = new ArrayList<Entry<String, RankObject>>();
		List<Entry<String, RankObject>> tokenPairLList = new ArrayList<Entry<String, RankObject>>();

		String docID = "";
		
		System.out.println(tokenPairList.size());
		int count = 0;
		
		for(Entry<String, RankObject> entry: tokenPairList){
			
			docID = entry.getKey();
			List<Integer> prevPosList = new ArrayList<Integer>();
			List<Integer> currPosList = new ArrayList<Integer>();
			boolean flag = false;
			
		   for(String word: searchArr){
			   
			   
			   
			   System.out.println(docID + "," + word);
			   
			   BasicDBObject query = new BasicDBObject();
		       query.put("word", word);
		       DBCursor cursor = coll.find(query);
		       
		       if (cursor.hasNext()) {   
		           DBObject obj = (DBObject) cursor.next();	                     
		           List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
		           
		           for(BasicDBObject docobj:temp){
		        	   
		        	   if(docobj.get("id").equals(docID)){
		        		   
		        		   currPosList = (List<Integer>)docobj.get("pos");
		        		   
		        		   if(!prevPosList.isEmpty()){
		        			   
		        			   for(int doc:currPosList){
		        				   
		        				   if(prevPosList.contains(doc-1)){
		        					   flag = true;
		        					   break;
		        				   }
		        				   else{
		        					   flag=false;
		        				   }
		        				   
		        			   }
		        			   
		        			   
		        		   }
		        		   else{
		        			   flag = true;
		        		   }
	        			   prevPosList.clear();
	        			   prevPosList.addAll(currPosList);
	        			   break;
		        	   }
		        	   
		           }
		       }
		       
		       if(!flag)
		    	   break;
		       
		   }
		   
		   if(flag){
				System.out.println("if " + entry.getKey());

			   tokenPairHList.add(entry);
		   }
		   else{
				System.out.println("else " + entry.getKey());

			   tokenPairLList.add(entry);
		   }
		}
		
		Collections.sort( tokenPairHList, new Comparator<Map.Entry<String, RankObject>>()
		{
			public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
			{
			    Double value2 = Double.valueOf(mapEntry2.getValue().tfidf_score);    
			    Double value1 = Double.valueOf(mapEntry1.getValue().tfidf_score);
			    
				return (value2).compareTo( value1 );
			}
		
		} );

		Collections.sort( tokenPairLList, new Comparator<Map.Entry<String, RankObject>>()
		{
			public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
			{
			    Double value2 = Double.valueOf(mapEntry2.getValue().tfidf_score);    
			    Double value1 = Double.valueOf(mapEntry1.getValue().tfidf_score);
			    
				return (value2).compareTo( value1 );
			}
		
		} );

		tokenPairList.clear();
		tokenPairList.addAll(tokenPairHList);
		tokenPairList.addAll(tokenPairLList);
		
		return tokenPairList;
	   
   }
   public void fnWriteDocMap(Map<String, docObject> docMap) throws UnknownHostException{
	   
	    Iterator itr = docMap.keySet().iterator();
	    while(itr.hasNext()){
	    	
	    	String id = (String)itr.next();
	    	docObject url_title = docMap.get(id);
	    	
            coll1.insert(new BasicDBObject("docID",id).
          		  append("url",url_title.URL).append("title",url_title.title));

	    }
	    
   }
   
   public void fnCalculateTFIDF() throws UnknownHostException{
	   

        DBCursor cursor = coll1.find();
        int N = cursor.length();
        System.out.println("N: " + cursor.length());
	   
	    DBCollection coll = db.getCollection("InvIndex");
        DBCursor cursor1 = coll.find();
        
        while(cursor1.hasNext()){
        	
            DBObject obj = (DBObject) cursor1.next();
            
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
            List<BasicDBObject> tempnew = new ArrayList<BasicDBObject>();
            int NT = temp.size();

            for(BasicDBObject tempInstObj: temp){
            	
            	
            	int tf = (int)tempInstObj.get("frequency");
            	
                double tfidf = (1+Math.log10(tf))*(Math.log10((float)N/NT));

                tempInstObj.put("tfidf", tfidf);
                tempnew.add(tempInstObj);
            	
            }
        	obj.put("doc", tempnew);
        	coll.save(obj);
        	
        }
   }
   
	 public void fnUpdateDocID(String filePath) throws FileNotFoundException, UnknownHostException{
		 
			FileReader inputFile = new FileReader(filePath);
			BufferedReader bufferread = new BufferedReader(inputFile);
			
			String eachline;
			int count=0; 
			
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
						
						File fileText = new File("/home/vijaykumar/IR_DUMP/" + textFileName + ".txt");

						String strTxt = "";
						
						if (fileText.exists()) {
							
							FileInputStream fis = new FileInputStream(fileText);
							byte[] data = new byte[(int) fileText.length()];
							fis.read(data);
							fis.close();

							strTxt = new String(data, "UTF-8");

						}
			         		
						if(count %100 == 0){
							System.out.println(count);
						}
						count++; 
						
						 BasicDBObject queryUrl = new BasicDBObject();
						 queryUrl.put("url", webLink);
						 DBCursor cursorUrl = coll1.find(queryUrl);
			           
						 if (cursorUrl.hasNext()) { 
							 
							DBObject objUrl = (DBObject) cursorUrl.next();
							objUrl.put("docname", textFileName);
							objUrl.put("text", strTxt);
							coll1.save(objUrl);
							
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
				try {
					
					if(bufferread != null)
						bufferread.close();
					
				}
				catch(IOException e){
					System.out.println(e);
				}
			}
	 

	 }

   
}