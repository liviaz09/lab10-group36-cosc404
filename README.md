# COSC 404 - Database System Implementation<br>Lab 10 - Recovering from a Database Failure

This lab explores recovery using a write-ahead log.

## Recovery with Write-Ahead Log (10 marks)

In this lab, you will implement undo-redo recovery using a write-ahead log.  The log records will be stored in a file and contain the data item values before/after an update and checkpoints.  Our data items will be identified by a single letter.  Your goal is to write the code that given a current state of the database will recover the database to the correct state based on the log file.

The code file to modify is `Recovery.java`.  Test using the JUnit test file `TestRecovery.java`.

## Submission

The lab can be marked immediately by the professor or TA by showing the output of the JUnit tests and by a quick code review.  Otherwise, submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**
