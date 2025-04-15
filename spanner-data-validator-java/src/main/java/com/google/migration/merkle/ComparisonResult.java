package com.google.migration.merkle;

import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(SerializableCoder.class)
public class ComparisonResult implements Serializable {

  public enum Status { MATCH, MISMATCH, SOURCE_MISSING }

  private String key;
  private Status status;
  private List<String> differingColumns; // Null or empty if MATCH or SOURCE_MISSING

  // Default constructor needed for AvroCoder/Serialization
  public ComparisonResult() {}

  public ComparisonResult(String key, Status status, List<String> differingColumns) {
    this.key = key;
    this.status = status;
    this.differingColumns = differingColumns;
  }

  // Getters...
  public String getKey() { return key; }
  public Status getStatus() { return status; }
  public List<String> getDifferingColumns() { return differingColumns; }

  @Override
  public String toString() {
    return "ComparisonResult{" +
        "key='" + key + '\'' +
        ", status=" + status +
        ", differingColumns=" + differingColumns +
        '}';
  }

  // Optional: equals/hashCode
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ComparisonResult that = (ComparisonResult) o;
    return Objects.equals(key, that.key) && status == that.status && Objects.equals(differingColumns, that.differingColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, status, differingColumns);
  }
}
