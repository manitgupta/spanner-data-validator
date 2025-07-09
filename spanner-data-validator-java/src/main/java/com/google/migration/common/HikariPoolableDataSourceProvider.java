package com.google.migration.common;

import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class HikariPoolableDataSourceProvider implements SerializableFunction<String, DataSource> {

  private static final ConcurrentHashMap<String, DataSource> instances =
      new ConcurrentHashMap<>();

  private HikariPoolableDataSourceProvider(
      String driverClassNameIn,
      List<JDBCShard> jdbcShardList,
      Integer maxConnectionsIn) {
    jdbcShardList.forEach(
        jdbcShard -> {
          instances.computeIfAbsent(jdbcShard.getJdbcUrl(), ignored -> {
            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl(jdbcShard.getJdbcUrl());
            ds.setUsername(jdbcShard.getUsername());
            ds.setPassword(jdbcShard.getPassword());
            ds.setDriverClassName(driverClassNameIn);
            ds.setMaximumPoolSize(maxConnectionsIn);
            //Various other connection pool settings.
            ds.setKeepaliveTime(30000);
            ds.setMaxLifetime(31000);
            ds.setConnectionTimeout(1000 * 3600 * 3);
            return ds;
          });
        }
    );
  }

  public static SerializableFunction<String, DataSource> of(
      String driverClassNameIn,
      List<JDBCShard> jdbcShardList,
      Integer maxConnectionsIn) {
    return new HikariPoolableDataSourceProvider(driverClassNameIn,
        jdbcShardList,
        maxConnectionsIn);
  }

  @Override
  public DataSource apply(String jdbcUrl) {
    return instances.get(jdbcUrl);
  }
}