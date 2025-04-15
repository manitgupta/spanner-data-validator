package com.google.migration.merkle;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import java.time.Instant;
import java.util.Objects;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class) // Use Avro for efficiency and schema evolution
public class MySqlRecord implements Serializable { // Renaming suggestion: Maybe just RecordData or similar if it holds Spanner data too

  private static final long serialVersionUID = 1L; // Recommended

  private Integer id; // Use Integer to allow null, or int if never null
  private String userName;
  private Double score; // Use Double or BigDecimal depending on precision needs
  private Instant lastActive; // Use modern Java Time

  // Default constructor REQUIRED for AvroCoder/Serialization
  public MySqlRecord() {}

  // Optional: Convenience constructor
  public MySqlRecord(Integer id, String userName, Double score, Instant lastActive) {
    this.id = id;
    this.userName = userName;
    this.score = score;
    this.lastActive = lastActive;
  }

  // --- Getters ---
  public Integer getId() { return id; }
  public String getUserName() { return userName; }
  public Double getScore() { return score; }
  public Instant getLastActive() { return lastActive; }

  // --- Setters ---
  public void setId(Integer id) { this.id = id; }
  public void setUserName(String userName) { this.userName = userName; }
  public void setScore(Double score) { this.score = score; }
  public void setLastActive(Instant lastActive) { this.lastActive = lastActive; }

  // Optional: equals and hashCode for testing or use in collections
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MySqlRecord that = (MySqlRecord) o;
    return Objects.equals(id, that.id) &&
        Objects.equals(userName, that.userName) &&
        Objects.equals(score, that.score) &&
        Objects.equals(lastActive, that.lastActive);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, userName, score, lastActive);
  }

  @Override
  public String toString() {
    return "MySqlRecord{" +
        "id=" + id +
        ", userName='" + userName + '\'' +
        ", score=" + score +
        ", lastActive=" + lastActive +
        '}';
  }
}