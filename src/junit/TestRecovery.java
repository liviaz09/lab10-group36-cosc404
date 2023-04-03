package junit;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import recovery.Database;
import recovery.Recovery;

/**
 * Tests recovery code using a write-ahead log.
 */
public class TestRecovery {

    private String filePath = "bin/data/";

    /**
     * Tests log1. (no checkpoints)
     */
    @Test
    public void testLog1() throws Exception {
        Recovery recovery = new Recovery();

        Database db = recovery.recover(filePath+"log1.csv");

        assertEquals("Database: {A=7, B=10, C=12, D=4} Recovery: Pass 1: [9, 0] Pass 2: [0, 7] Pass 3: [9, 3]",
                db.toString());
    }

    /**
     * Tests log2. (Checkpoint started but not completed)
     */
    @Test
    public void testLog2() throws Exception {
        Recovery recovery = new Recovery();

        Database db = recovery.recover(filePath+"log2.csv");

        assertEquals("Database: {A=7, B=10, C=12, D=8, E=0} Recovery: Pass 1: [15, 0] Pass 2: [0, 14] Pass 3: [15, 3]",
                db.toString());
    }

    /**
     * Tests log3. (Checkpoint started and completed)
     */
    @Test
    public void testLog3() throws Exception {
        Recovery recovery = new Recovery();

        Database db = recovery.recover(filePath+"log3.csv");

        assertEquals("Database: {A=5, B=3, C=5, D=8, E=0} Recovery: Pass 1: [18, 9] Pass 2: [9, 17] Pass 3: [18, 12]",
                db.toString());
    }

    /**
     * Tests log4. (Checkpoint started and completed and another checkpoint started)
     */
    @Test
    public void testLog4() throws Exception {
        Recovery recovery = new Recovery();

        Database db = recovery.recover(filePath+"log4.csv");

        assertEquals("Database: {A=5, B=3, C=11, D=2, E=10} Recovery: Pass 1: [23, 9] Pass 2: [9, 23] Pass 3: [23, 19]",
                db.toString());
    }
}
