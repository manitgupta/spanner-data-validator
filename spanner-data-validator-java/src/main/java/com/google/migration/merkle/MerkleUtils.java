package com.google.migration.merkle;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
// Remove java.sql.Timestamp import if no longer needed directly here
import java.time.Instant; // Preferred modern type
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*; // For List, Map, Base64, etc.

public class MerkleUtils implements Serializable {

  // Define constants for column types (still useful for canonicalizeValue)
  public static final String TYPE_STRING = "VARCHAR";
  public static final String TYPE_INT = "INT";
  public static final String TYPE_FLOAT = "FLOAT"; // Or DOUBLE, DECIMAL
  public static final String TYPE_TIMESTAMP = "TIMESTAMP"; // Represents Instant now
  public static final String TYPE_BYTES = "BYTES";

  private static final String NULL_REPRESENTATION = "<<NULL>>";
  private static final DateTimeFormatter ISO_UTC_FORMATTER =
      DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
  private static final int DECIMAL_SCALE = 3;

  private static final ThreadLocal<MessageDigest> SHA_256_DIGEST =
      ThreadLocal.withInitial(() -> {
        try {
          return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
          throw new RuntimeException("SHA-256 algorithm not available", e);
        }
      });

  /**
   * Converts a value to its canonical string form based on type.
   * Now expects Instant for TYPE_TIMESTAMP.
   */
  public static String canonicalizeValue(Object value, String columnType) {
    if (value == null) {
      return NULL_REPRESENTATION;
    }

    try {
      switch (columnType.toUpperCase()) {
        case TYPE_STRING:
          return ((String) value).trim();
        case TYPE_INT: // Handles Integer, Long, etc.
          return value.toString();
        case TYPE_FLOAT: // Handles Float, Double, BigDecimal
          BigDecimal bd = new BigDecimal(value.toString());
          return bd.setScale(DECIMAL_SCALE, RoundingMode.HALF_UP).toPlainString();
        case TYPE_TIMESTAMP: // Expecting java.time.Instant
          if (value instanceof Instant) {
            // Format consistently to UTC ISO 8601
            return ISO_UTC_FORMATTER.format((Instant) value);
          } else {
            // Maybe handle conversion from Long epoch ms if needed, but primarily expect Instant
            throw new IllegalArgumentException("Expected java.time.Instant for TIMESTAMP type, got: " + value.getClass());
          }
        case TYPE_BYTES:
          if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
          } else {
            throw new IllegalArgumentException("Expected byte[] for BYTES type, got: " + value.getClass());
          }

        default:
          return value.toString();
      }
    } catch (Exception e) {
      System.err.println("Error canonicalizing value '" + value + "' of type '" + columnType + "': " + e.getMessage());
      return "CANONICALIZATION_ERROR:" + value;
    }
  }

  /**
   * Computes the SHA-256 hash of the input string and returns it as an uppercase Hex string.
   * (No changes needed here)
   */
  public static String hash(String data) {
    // ... (implementation from previous version using StringBuilder) ...
    if (data == null) {
      data = ""; // Or handle null data consistently
    }
    MessageDigest digest = SHA_256_DIGEST.get();
    digest.reset(); // Reset digest state
    byte[] hashBytes = digest.digest(data.getBytes(StandardCharsets.UTF_8));

    StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
    for (byte b : hashBytes) {
      hexString.append(String.format("%02X", b));
    }
    return hexString.toString();
  }


  /**
   * Builds a Merkle tree for the MySqlRecord POJO.
   * No longer needs columnTypes map as type is inferred from POJO fields.
   *
   * @param record       The POJO record
   * @param columnOrder  Fixed order of columns to process (must match POJO fields)
   * @return MerkleTreeResult containing root and leaf hashes
   */
  public static MerkleTreeResult buildMerkleTree(MySqlRecord record,
      List<String> columnOrder) { // Removed columnTypes
    if (record == null) {
      return new MerkleTreeResult(null, Collections.emptyMap());
    }

    Map<String, String> leafHashes = new LinkedHashMap<>();
    List<String> orderedHashes = new ArrayList<>();

    // Process fields based on columnOrder, calling canonicalizeValue with known types
    for (String colName : columnOrder) {
      Object value = null;
      String colType = TYPE_STRING; // Default, will be overridden

      // Get value and determine type based on column name matching POJO field
      switch (colName) {
        case "id":
          value = record.getId();
          colType = TYPE_INT;
          break;
        case "user_name": // Match the actual field name or use a mapping
          value = record.getUserName();
          colType = TYPE_STRING;
          break;
        case "score":
          value = record.getScore();
          colType = TYPE_FLOAT;
          break;
        case "last_active": // Match the actual field name
          value = record.getLastActive();
          colType = TYPE_TIMESTAMP;
          break;
        // Add cases for other fields if the POJO grows
        default:
          // Handle unknown column name in order list? Log warning?
          System.err.println("Warning: Unknown column '" + colName + "' in columnOrder for Merkle tree building.");
          continue; // Skip unknown columns
      }

      String canonicalStr = canonicalizeValue(value, colType);
      String leafHash = hash(canonicalStr);
      leafHashes.put(colName, leafHash);
      orderedHashes.add(leafHash);
    }

    if (orderedHashes.isEmpty()) {
      return new MerkleTreeResult(hash(""), Collections.emptyMap());
    }

    // Build the tree levels (no changes needed here)
    List<String> currentLevelHashes = new ArrayList<>(orderedHashes);
    while (currentLevelHashes.size() > 1) {
      // ... (pairwise hashing logic remains the same) ...
      List<String> nextLevelHashes = new ArrayList<>();
      for (int i = 0; i < currentLevelHashes.size(); i += 2) {
        String left = currentLevelHashes.get(i);
        String right = (i + 1 < currentLevelHashes.size()) ? currentLevelHashes.get(i + 1) : left;
        String parentHash = hash(left + right);
        nextLevelHashes.add(parentHash);
      }
      currentLevelHashes = nextLevelHashes;
    }

    String rootHash = currentLevelHashes.get(0);
    return new MerkleTreeResult(rootHash, leafHashes);
  }
}