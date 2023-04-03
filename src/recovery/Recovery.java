package recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
    	
		// for ( )
		{
			//		case CHECKPOINT_START:
						// TODO: Add all transactions not currently in REDO LIST to UNDO LIST
							
						
			// 		case CHECKPOINT_END:    			 
						// TODO: Put all data values into database.  Comma-separate key value pairs
												 
		}
		
		// TODO: Record start and end of pass #1 in db    	
		// db.setEndPass(1, ?TODO:?);
		// db.setStartPass(1, ?TODO:?);
		
		// TODO: Perform REDO pass #2
		// Pass #2: REDO from start of log (or CHECKPOINT START with matching CHECKPOINT END) until have redone all operations for transactions in redo list
		
		// TODO: Record start and end of pass #2

		
		// TODO: Perform UNDO Pass #3
		// Pass #3: UNDO from end of log until have undone all operations for transactions in undo list
				
		// TODO: Record start and end of pass #3
		
		System.out.println(db);
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
