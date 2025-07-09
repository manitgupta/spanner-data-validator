package com.google.migration.common;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class)
public class JDBCShard implements Serializable {
  private final String jdbcUrl;
  private final String username;
  private final String password;

  public JDBCShard(String jdbcUrl, String username, String password) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
