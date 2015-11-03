package com.expedia.mongoMigration;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class MongoConnection {
	
	private static MongoClient mongoClient;
	
	private static final String CHECKS_COLL_NAME = "checks";
	
	private static DB db;
	
	public MongoConnection(String dbURL, String databaseName, String userName, String password){
		try {
			if (userName == null){
				mongoClient = new MongoClient( dbURL, 27017 );
			}
			else {
				MongoCredential credential = MongoCredential.createCredential(userName, databaseName, password.toCharArray());
				mongoClient = new MongoClient(new ServerAddress(), Arrays.asList(credential));
			}
			db = mongoClient.getDB( "mydb" );
			mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		}
		catch (Exception e){
			System.out.println("Unable to instantiate Mongo client using the follwing parameters: ");
			System.out.println("    username - " + userName);
			System.out.println("    password - " + password);
			System.out.println("    database name - " + databaseName);
			e.printStackTrace();
		}
	}
	
	public boolean dropCurrentChecks(){
		DBCollection checks = this.getCollection(CHECKS_COLL_NAME);
		try {
			checks.drop();
			return true;
		}
		catch (Exception e){
			System.out.println("Errors encountered while dropping current checks collection.");
			e.printStackTrace();
			return false;
		}
	}
	
	public DBObject[] getChecks(){
		DBCollection checks = this.getCollection(CHECKS_COLL_NAME);
		ArrayList<DBObject> allChecks = new ArrayList<DBObject>();
		DBCursor cursor = checks.find();
		while (cursor.hasNext()){
			DBObject check = cursor.next();
			allChecks.add(check);
		}
		cursor.close();
		DBObject[] checkArray = new DBObject[allChecks.size()];
		allChecks.toArray(checkArray);
		return checkArray;
	}
	
	public boolean disableCheck(DBObject check){
		try {
			check.put("enabled", false);
			return true;
		}
		catch (Exception e){
			System.out.println("Failed to disable check #" + check.get("_id"));
			return false;
		}
	}
	

	private DBCollection getCollection(String collectionName){
		DBCollection coll = db.getCollection(collectionName);
		return coll;
	}
	
	
	public boolean convertCheckSubscriptions(DBObject check, boolean disable){
		try {
			BasicDBList subscriptions = (BasicDBList) check.get("subscriptions");
			if (subscriptions != null){
				if (subscriptions.size() > 0){
					BasicDBList convertedSubscriptions = convertCheckSubscriptions(subscriptions, disable);
					check.put("subscriptions", convertedSubscriptions);
				}
			}
			return true;
		}
		catch (Exception e){
			System.out.println("Errors encountered during subscription conversion on check #:" + check.get("_id"));
			return true;
		}
	}
	
	private BasicDBList convertCheckSubscriptions(BasicDBList subscriptions, boolean disable){
		BasicDBList convertedSubscriptions = new BasicDBList();
		Iterator<Object> iterator = subscriptions.iterator();
		while (iterator.hasNext()){
			DBObject subscription = null;
			try {
				subscription = (DBObject)iterator.next();
				String type = (String)subscription.get("type");
				if (type.equals("EOS")){
					subscription.put("type", "SCRIPT");
					String oldTarget = (String)subscription.get("target");
					String newTarget = cutOffPort(oldTarget);
					subscription.put("target", newTarget);
				}
				if (disable) {
					subscription.put("enabled", false);
				}
				convertedSubscriptions.add(subscription);
			}
			catch (Exception e){
				System.out.println("Errors encountered during conversion of subscription #:" + subscription.get("_id"));
			}
		}
		return convertedSubscriptions;
	}
	
	private String cutOffPort(String target){
		int portLocation = target.indexOf(":8000");
		if (portLocation > 0){
			return target.substring(0, portLocation) + ":8080";
		}
		else {
			return target;
		}
	}
	
	public boolean saveCheck(DBObject check){
		try {
			DBCollection checks = this.getCollection(CHECKS_COLL_NAME);
			checks.save(check);
			return true;
		}
		catch (Exception e){
			System.out.println("Check not saved, id: #" + check.get("id"));
			return false;
		}
	}
}