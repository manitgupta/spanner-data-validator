package com.google.migration.merkle;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

// Assuming Key is String, now comparing MySqlRecord objects
public class CompareRecordsDoFn extends DoFn<KV<String, CoGbkResult>, KV<String, ComparisonResult>> {

  private static final Logger LOG = LoggerFactory.getLogger(CompareRecordsDoFn.class);

  // Update TupleTag types to use the POJO
  private final TupleTag<MySqlRecord> mysqlTag;
  private final TupleTag<MySqlRecord> spannerTag;
  private final List<String> columnOrder; // Still needed for consistent tree building

  // Constructor updated - no longer needs columnTypes map for MerkleUtils call
  public CompareRecordsDoFn(TupleTag<MySqlRecord> mysqlTag,
      TupleTag<MySqlRecord> spannerTag,
      List<String> columnOrder) {
    this.mysqlTag = mysqlTag;
    this.spannerTag = spannerTag;
    this.columnOrder = columnOrder;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    KV<String, CoGbkResult> element = c.element();
    String key = element.getKey();
    CoGbkResult result = element.getValue();

    // Extract POJOs using the correct TupleTags
    MySqlRecord mysqlRecord = StreamSupport.stream(result.getAll(mysqlTag).spliterator(), false)
        .findFirst().orElse(null);
    MySqlRecord spannerRecord = StreamSupport.stream(result.getAll(spannerTag).spliterator(), false)
        .findFirst().orElse(null);


    if (mysqlRecord == null || spannerRecord == null) {
      LOG.warn("Missing record for key {}. MySQL found: {}, Spanner found: {}",
          key, mysqlRecord != null, spannerRecord != null);
      c.output(KV.of(key, new ComparisonResult(key, ComparisonResult.Status.SOURCE_MISSING, null)));
      return;
    }

    try {
      // Build Merkle trees using the POJOs and columnOrder
      // No longer passing columnTypes map
      MerkleTreeResult mysqlTree = MerkleUtils.buildMerkleTree(mysqlRecord, columnOrder);
      MerkleTreeResult spannerTree = MerkleUtils.buildMerkleTree(spannerRecord, columnOrder);

      // Comparison logic remains the same
      if (Objects.equals(mysqlTree.getRootHash(), spannerTree.getRootHash())) {
        c.output(KV.of(key, new ComparisonResult(key, ComparisonResult.Status.MATCH, null)));
      } else {
        List<String> diffCols = new ArrayList<>();
        Map<String, String> mysqlLeaves = mysqlTree.getLeafHashes();
        Map<String, String> spannerLeaves = spannerTree.getLeafHashes();

        for (String colName : columnOrder) {
          String mysqlLeafHash = mysqlLeaves.get(colName);
          String spannerLeafHash = spannerLeaves.get(colName);
          if (!Objects.equals(mysqlLeafHash, spannerLeafHash)) {
            diffCols.add(colName);
          }
        }
        LOG.info("Mismatch found for key {}. Differing columns: {}", key, diffCols);
        c.output(KV.of(key, new ComparisonResult(key, ComparisonResult.Status.MISMATCH, diffCols)));
      }
    } catch (Exception e) {
      LOG.error("Error comparing records for key {}: {}", key, e.getMessage(), e);
      c.output(KV.of(key, new ComparisonResult(key, ComparisonResult.Status.SOURCE_MISSING, List.of("COMPARISON_ERROR"))));
    }
  }
}