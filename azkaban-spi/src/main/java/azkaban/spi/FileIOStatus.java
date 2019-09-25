package azkaban.spi;

// OPEN = file exists, is open and currently being written to
// CLOSED = file exists, is closed and has been finalized (not being written to).
// NON_EXISTANT = the file does not exist
public enum FileIOStatus {
  OPEN, CLOSED, NON_EXISTANT
}
