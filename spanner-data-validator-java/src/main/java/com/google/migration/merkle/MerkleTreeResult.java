package com.google.migration.merkle;

import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

// Use AvroCoder for better schema evolution support in Beam, or ensure Serializable
@DefaultCoder(SerializableCoder.class)
public class MerkleTreeResult implements Serializable { // Implement Serializable as a fallback coder mechanism
  private String rootHash;
  private Map<String, String> leafHashes; // column name -> hash

  // Default constructor needed for AvroCoder/Serialization
  public MerkleTreeResult() {}

  public MerkleTreeResult(String rootHash, Map<String, String> leafHashes) {
    this.rootHash = rootHash;
    this.leafHashes = leafHashes;
  }

  public String getRootHash() {
    return rootHash;
  }

  public Map<String, String> getLeafHashes() {
    return leafHashes;
  }

  // Optional: equals and hashCode for potential use in testing or collections
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MerkleTreeResult that = (MerkleTreeResult) o;
    return Objects.equals(rootHash, that.rootHash) &&
        Objects.equals(leafHashes, that.leafHashes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rootHash, leafHashes);
  }

  @Override
  public String toString() {
    return "MerkleTreeResult{" +
        "rootHash='" + rootHash + '\'' +
        ", leafHashes=" + leafHashes +
        '}';
  }
}
