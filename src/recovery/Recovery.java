package recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import recovery.LogRecord.LogRecordType;

/**
 * Reads a write-ahead log and uses it to restore the database into a consistent
 * state.
 */
public class Recovery {

	private static String filePath = "bin/data/";

	/**
	 * Main method
	 * 
	 * @param args
	 *             no arguments required
	 */
	public static void main(String[] args) throws Exception {
		Recovery recovery = new Recovery();

		recovery.recover(filePath+"log1.csv");
		recovery.recover(filePath+"log2.csv");
		recovery.recover(filePath+"log3.csv");
		recovery.recover(filePath+"log4.csv");
	}

	/**
	 * Given a write-ahead log, returns the database after recovery.
	 * 
	 * @return
	 *         connection
	 */
	public Database recover(String fileName) {
		System.out.println("\nPerforming recovery on WAL: " + fileName);

		Database db = new Database();

		// Read the log records into an ArrayList from a file
		ArrayList<LogRecord> records = readLog(fileName);

		// Create UNDO and REDO lists
		HashSet<String> undoList = new HashSet<String>();
		HashSet<String> redoList = new HashSet<String>();

		// Pass #1: Determine undo and redo lists.
		//          Start scan at end of log and go towards front of log until hit start of log or checkpoint start with matching checkpoint end.
		// TODO: Implement loop to scan through log records for Pass #1
		// TODO: For a checkpoint start, all active transactions are in transaction field of log record in a comma-separated form    	
		// TODO: For a checkpoint end, the value saved to database for each item is given.  These key-value pairs are comma-separated in transaction field in record.
		// set end position:
			int endPassOne = 0;
			boolean checkEnd = false;
			pass1 : for (int i = records.size()-1; i >= 0; i--) 
			{
				LogRecord record = records.get(i);

				String transactions = record.getTransaction(); //all active transactions	
				//System.out.println(record.getType());
				
					// TODO: Add all transactions not currently in REDO LIST to UNDO LIST
				if(record.getType() == LogRecordType.COMMIT){
					if(!redoList.contains(record.getTransaction())){
							redoList.add(record.getTransaction());
					}
						// we add updated value to the databse
				}
					//System.out.println(record.getUpdatedValue());
				else if(record.getType() == LogRecordType.CHECKPOINT_START){
					
					if(!redoList.contains(record.getTransaction())){
						undoList.add(record.getTransaction());
					}
					if(checkEnd){
						endPassOne =i;
						break pass1;
					}
				}
				else if(record.getType()==LogRecordType.CHECKPOINT_END ){
					checkEnd =true;
					//add to database
					String valueUpdate [] = record.getTransaction().split(",");
					for(int j =0; j < valueUpdate.length;j+=2){
						String  key = valueUpdate[j];
						String letter = valueUpdate[j+1];
						db.put(key,Integer.parseInt(letter));

					}
				}
				else{
					if(!redoList.contains(record.getTransaction())){
					undoList.add(record.getTransaction());{
					}
					}
				}										 
			}	
			db.setEndPass(1, endPassOne);
			db.setStartPass(1, records.size()-1);
			
	// TODO: Perform REDO pass #2
		db.setStartPass(2, endPassOne);
		int endPassTwo =endPassOne;

		for(int i =endPassOne; i<records.size();i++){
			LogRecord rec = records.get(i);
			if(redoList.contains(rec.getTransaction())){
				if(rec.getItem()!=null){
				db.put(rec.getItem(),rec.getUpdatedValue());
				endPassTwo = i;
				}
			}

		}
		db.setEndPass(2, endPassTwo);

		
		// TODO: Perform UNDO Pass #3
		// Pass #3: UNDO from end of log until have undone all operations for transactions in undo list
		db.setStartPass(3, endPassTwo);		
		// TODO: Record start and end of pass #3
		for(int i= endPassTwo;i>= records.size();i--){
			LogRecord rec = records.get(i);
			if(undoList.contains(rec.getTransaction())){
				if(rec.getType()==LogRecordType.UPDATE){
					db.put(rec.getItem(),rec.getInitialValue());
				}
			}
		}
		db.setEndPass(endPassTwo, records.size()-1);
		return db;
	}

	/**
	 * Reads a log file where each record is comma-separated and on its own line.
	 * 
	 * @param fileName
	 *                 file name and path to read
	 * @return
	 *         ArrayList of LogRecords
	 */
	public ArrayList<LogRecord> readLog(String fileName) {
		ArrayList<LogRecord> records = new ArrayList<LogRecord>();

		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String rec;

			while ((rec = reader.readLine()) != null) {
				LogRecord record = LogRecord.parse(rec);
				records.add(record);
			}
		} catch (Exception e) {
			System.out.println("File exception: " + e);
			e.printStackTrace();
			System.exit(1);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					System.out.println(e);
				}
			}
		}

		System.out.println(records);
		return records;
	}
}
