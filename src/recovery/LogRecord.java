package recovery;

/**
 * Represents a log record in a write-ahead log.
 */
public class LogRecord {
	public enum LogRecordType {
		INIT, START, COMMIT, ABORT, UPDATE, CHECKPOINT_START, CHECKPOINT_END
	}

	/**
	 * Type of log record
	 */
	private LogRecordType type;

	/**
	 * Transaction name: T0, T1, T2, ...
	 */
	private String transaction;

	/**
	 * Data item updated: A, B, C, ...
	 */
	private String item;

	/**
	 * Value before update
	 */
	private int initialValue;

	/**
	 * Value after update
	 */
	private int updatedValue;

	/**
	 * Gets initial value before update in update log record.
	 * 
	 * @return
	 *         initial value
	 */
	public int getInitialValue() {
		return initialValue;
	}

	/**
	 * Gets the data item name.
	 * 
	 * @return
	 *         data item name
	 */
	public String getItem() {
		return item;
	}

	/**
	 * Gets the transaction id.
	 * 
	 * @return
	 *         transaction id
	 */
	public String getTransaction() {
		return transaction;
	}

	/**
	 * Gets the log record type.
	 * 
	 * @return
	 *         log record type
	 */
	public LogRecordType getType() {
		return type;
	}

	/**
	 * Gets updated value after update in update log record.
	 * 
	 * @return
	 *         updated (after) value
	 */
	public int getUpdatedValue() {
		return updatedValue;
	}

	/**
	 * Sets initial value before update in update log record.
	 * 
	 * @param initialValue
	 *                     initial value
	 */
	public void setInitialValue(int initialValue) {
		this.initialValue = initialValue;
	}

	/**
	 * Sets the data item name.
	 * 
	 * @param item
	 *             item name
	 */
	public void setItem(String item) {
		this.item = item;
	}

	/**
	 * Sets the transaction id.
	 * 
	 * @param transaction
	 *                    transaction id
	 */
	public void setTransaction(String transaction) {
		this.transaction = transaction;
	}

	/**
	 * Sets the log record type.
	 * 
	 * @param type
	 *             log record type
	 */
	public void setType(LogRecordType type) {
		this.type = type;
	}

	/**
	 * Sets updated value after update in update log record.
	 * 
	 * @param updatedValue
	 *                     updated (after) value
	 */
	public void setUpdatedValue(int updatedValue) {
		this.updatedValue = updatedValue;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append("(");
		buf.append(this.type);
		if (this.transaction != null) {
			buf.append(", ");
			buf.append(this.transaction);
		}

		if (type == LogRecordType.UPDATE || type == LogRecordType.INIT) {
			buf.append(", ");
			buf.append(this.item);
			buf.append(", ");
			buf.append(this.initialValue);
			if (type == LogRecordType.UPDATE) {
				buf.append(", ");
				buf.append(this.updatedValue);
			}
		}
		buf.append(")");
		return buf.toString();
	}

	/**
	 * Given a log record in string (CSV) form, parses and creates a LogRecord
	 * object.
	 * 
	 * Log record format: type, transactionId, dataItemName, initialValue,
	 * updatedValue
	 * 
	 * Note that only if the item is an update will it have the last 3 fields.
	 * 
	 * @param record
	 *               record in string (CSV) form
	 * @return
	 *         LogRecord object
	 */
	public static LogRecord parse(String record) {
		LogRecord logrec = new LogRecord();

		String[] components = record.split(",");

		if (components.length == 0)
			return null;

		switch (components[0].charAt(0)) {
			case 'I':
				logrec.setType(LogRecordType.INIT);
				logrec.setItem(components[2]);
				logrec.setInitialValue(Integer.parseInt(components[3]));
				if (components.length > 1)
					logrec.setTransaction(components[1]);
				break;

			case 'S':
				logrec.setType(LogRecordType.START);
				if (components.length > 1)
					logrec.setTransaction(components[1]);
				break;

			case 'C':
				// May be CHECKPOINT_START (CS) or CHECKPOINT_END (CE) or COMMIT (C)
				if (components[0].length() == 1) {
					logrec.setType(LogRecordType.COMMIT);
					if (components.length > 1)
						logrec.setTransaction(components[1]);
				} else {
					if (components[0].equals("CS")) { // CHECKPOINT_START
						logrec.setType(LogRecordType.CHECKPOINT_START);
						// Note: The list of transactions are put back as comma-separated into
						// transaction field
						StringBuilder buf = new StringBuilder();
						for (int i = 1; i < components.length; i++) {
							if (i != 1)
								buf.append(",");
							buf.append(components[i]);
						}
						logrec.setTransaction(buf.toString());
					} else if (components[0].equals("CE")) { // CHECKPOINT_END
						logrec.setType(LogRecordType.CHECKPOINT_END);
						// Note: The list of all current data items and values are put back as
						// comma-separated into transaction field
						StringBuilder buf = new StringBuilder();
						for (int i = 1; i < components.length; i++) {
							if (i != 1)
								buf.append(",");
							buf.append(components[i]);
						}
						logrec.setTransaction(buf.toString());
					}
				}

				break;

			case 'A':
				logrec.setType(LogRecordType.ABORT);
				if (components.length > 1)
					logrec.setTransaction(components[1]);
				break;

			case 'U':
				logrec.setType(LogRecordType.UPDATE);
				logrec.setItem(components[2]);
				logrec.setInitialValue(Integer.parseInt(components[3]));
				logrec.setUpdatedValue(Integer.parseInt(components[4]));
				if (components.length > 1)
					logrec.setTransaction(components[1]);
				break;
			default:
				System.out.println("Error.  Invalid type: " + components[0]);
		}

		return logrec;
	}
}
