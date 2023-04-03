package recovery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Our database will consist of objects with a string name and an integer value.
 * 
 * Underlying data structure is a HashMap.
 */
public class Database extends HashMap<String, Integer> {
	/*
	 * Note: Use the HashMap methods get/put to retrieve and set values of the
	 * database.
	 */

	/**
	 * Array of indexes in log record list (starting from 0) where start pass 1, 2,
	 * and 3.
	 */
	private int[] startPasses;

	/**
	 * Array of indexes in log record list (starting from 0) where end pass 1, 2,
	 * and 3.
	 */
	private int[] endPasses;

	/**
	 * Constructor.
	 */
	public Database() {
		super();
		startPasses = new int[3];
		endPasses = new int[3];
	}

	@Override
	public String toString() {
		// Output datda items in sorted order
		List<String> keys = new ArrayList<String>(this.keySet());
		Collections.sort(keys);

		StringBuilder buf = new StringBuilder();
		buf.append("Database: ");

		boolean first = true;
		buf.append("{");
		for (String s : keys) {
			if (!first)
				buf.append(", ");
			first = false;
			buf.append(s);
			buf.append("=");
			buf.append(this.get(s));
		}
		buf.append("}");
		buf.append(" Recovery: Pass 1: [" + startPasses[0] + ", " + endPasses[0]
				+ "] Pass 2: [" + startPasses[1] + ", " + endPasses[1] + "] Pass 3: ["
				+ startPasses[2] + ", " + endPasses[2] + "]");
		return buf.toString();
	}

	/**
	 * Gets the location of the start of each of the three recovery passes.
	 * 
	 * @return
	 *         start locations of passes
	 */
	public int[] getStartPasses() {
		return startPasses;
	}

	/**
	 * Gets the location of the end of each of the three recovery passes.
	 * 
	 * @return
	 *         end locations of passes
	 */
	public int[] getEndPasses() {
		return endPasses;
	}

	/**
	 * Sets the start location of the given pass.
	 * 
	 * @param passnum
	 *                pass number (starting at 1)
	 * @param loc
	 *                start location (indexed from 0)
	 */
	public void setStartPass(int passnum, int loc) {
		startPasses[passnum - 1] = loc;
	}

	/**
	 * Sets the end location of the given pass.
	 * 
	 * @param passnum
	 *                pass number (starting at 1)
	 * @param loc
	 *                end location (indexed from 0)
	 */
	public void setEndPass(int passnum, int loc) {
		endPasses[passnum - 1] = loc;
	}
}
