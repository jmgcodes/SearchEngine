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
import java.math.*;




public class MongoDB{
	
	static MongoClient mongoClient;
	static DB db;
	static DBCollection coll;
	static DBCollection coll1;
	
	MongoDB() throws UnknownHostException {
	  // To connect to mongodb server
		mongoClient = new MongoClient( "localhost" , 27017 );
    // Now connect to your databases
		db = mongoClient.getDB("InvertedIndex");
   // System.out.println("Connect to database successfully");
		coll = db.getCollection("InvIndex");
   // System.out.println("Collection mycol selected successfully");
	    coll1 = db.getCollection("DocMap");
	}
	
   public  void fnMongo(Map<String,  List<posObj>> wordMap){
      try{   
    	  
          int count = 0;
          
          Map<String, Map<String, List<Integer>>> wordList = new HashMap<String, Map<String, List<Integer>>>();
          List<posObj> docList = new ArrayList<posObj>();
          posObj pos = new posObj();
          
          Map<String, posObj> docListDB = new HashMap<String, posObj>();
          posObj posDB = new posObj();

          System.out.println("Mapsize: " + wordMap.size());

          Iterator iterMap = wordMap.keySet().iterator();
          while(iterMap.hasNext()){
        	  
        	  if(count%1000==0)
        		  System.out.println("Storing word " + count + " to " + count+1000);
        		  
        	  count++;

        	  String word = (String)iterMap.next();
        	  docList = wordMap.get(word);
        	  
              BasicDBObject query = new BasicDBObject();
              query.put("word", word);
              
              DBCursor cursor = coll.find(query);
              if (cursor.hasNext()) { 
            	  
                  DBObject obj = (DBObject) cursor.next();
                  
                 //System.out.println("Found " + obj.get("word") + " " + obj.get("doc")); 
                 
                  List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

                  for(posObj tempposobj: docList){
                   	  temp.add(new BasicDBObject("id",tempposobj.getDocID()).append("pos", tempposobj.getPos()).append("frequency", tempposobj.getFreq()));
                  }
                 obj.put("doc", temp);
                 coll.save(obj); 
                 
              }
              else{
            	  
                  //System.out.println("Create " + word);
                  
            	  List<BasicDBObject> listObj = new ArrayList<BasicDBObject>();
            	  
                  for(posObj tempposobj: docList){
                	  listObj.add(new BasicDBObject("id",tempposobj.getDocID()).append("pos", tempposobj.getPos()).append("frequency", tempposobj.getFreq()));
                  }
                  
                  coll.insert(new BasicDBObject("doc",listObj).
                		  append("word",word));

            	  
              }

        	  
          }
//          mongoClient.close();
          
      }catch(Exception e){
	     System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	  }
      
   }
   
   public void fnFind(String search) throws UnknownHostException{
	   
	   // To connect to mongodb server
//	    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	    // Now connect to your databases
//	    DB db = mongoClient.getDB("InvertedIndex");
	   // System.out.println("Connect to database successfully");
//	    DBCollection coll = db.getCollection("InvIndex");
//	    DBCollection coll1 = db.getCollection("DocMap");

	   // System.out.println("Collection mycol selected successfully");
	    
        BasicDBObject query = new BasicDBObject();
        query.put("word", search);
        
        DBCursor cursor = coll.find(query);
        if (cursor.hasNext()) { 
      	  
            DBObject obj = (DBObject) cursor.next();
                      
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

            for(BasicDBObject tempObj: temp){
            	
            	//tempObj = (BasicDBObject)tempObj.get("docInst");
            	
                BasicDBObject queryUrl = new BasicDBObject();
                queryUrl.put("docID", tempObj.get("id"));
                DBCursor cursorUrl = coll1.find(queryUrl);
                
                if (cursorUrl.hasNext()) { 
              	  
                    DBObject objUrl = (DBObject) cursorUrl.next();
                              
                	System.out.println("Document: " + objUrl.get("url"));
                }
                
            	
            }
     	 
        }
        else{
        	System.out.println("Sorry! Word not found");

        }
        
//        mongoClient.close();
   }
   
   
   public void fnWriteDocMap(Map<String, String> docMap) throws UnknownHostException{
	   
	   // To connect to mongodb server
//	    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	    // Now connect to your databases
//	    DB db = mongoClient.getDB("InvertedIndex");
	   // System.out.println("Connect to database successfully");
//	    DBCollection coll = db.getCollection("DocMap");
	   // System.out.println("Collection mycol selected successfully");

	    Iterator itr = docMap.keySet().iterator();
	    while(itr.hasNext()){
	    	
	    	String id = (String)itr.next();
	    	String url = docMap.get(id);
	    	
            coll1.insert(new BasicDBObject("docID",id).
          		  append("url",url));

	    }
	    
//	    mongoClient.close();
   }
   
   public void fnCalculateTFIDF() throws UnknownHostException{
	   
	   // To connect to mongodb server
//	    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	    // Now connect to your databases
//	    DB db = mongoClient.getDB("InvertedIndex");
	   // System.out.println("Connect to database successfully");
//	    DBCollection coll = db.getCollection("DocMap");
	   // System.out.println("Collection mycol selected successfully");

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
            	
            	//BasicDBObject tempInstObj = (BasicDBObject)tempObj.get("docInst");
            	
            	int tf = (int)tempInstObj.get("frequency");
            	
               // System.out.println("Word: " + obj.get("word")+" || NT: " + NT + " || TF: " + tf);
                double tfidf = (Math.log(1+tf))*(Math.log((float)N/NT));

                tempInstObj.put("tfidf", tfidf);
                tempnew.add(tempInstObj);
            	
            }
        	obj.put("doc", tempnew);
        	coll.save(obj);
        	
        }
//        mongoClient.close();

        
   }
   
}