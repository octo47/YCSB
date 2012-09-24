/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;


import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.hbase.async.*;
import org.hbase.async.Scanner;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HBase client for YCSB framework
 */
public class AsyncHBaseClient extends com.yahoo.ycsb.DB {
  public static final Charset UTF8 = Charset.forName("UTF-8");
  public boolean _debug = false;

  public String _columnFamily = "";
  public byte _columnFamilyBytes[];
  public String _zkQuorum;
  public String _zkBasePath;
  public int _batchsize;
  public boolean _buffered;
  public boolean _durable;
  public HBaseClient _client;


  public static final int Ok = 0;
  public static final int ServerError = -1;

  private AtomicInteger _mutations = new AtomicInteger();
  private boolean _throttle;
  private boolean _failed;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    if ((getProperties().getProperty("debug") != null) &&
            (getProperties().getProperty("debug").compareTo("true") == 0)) {
      _debug = true;
    }

    _columnFamily = getProperties().getProperty("columnfamily");
    if (_columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    _columnFamilyBytes = Bytes.UTF8(_columnFamily);
    _zkQuorum = getProperties().getProperty("zkquorum");
    _zkBasePath = getProperties().getProperty("zkpath", "/hbase");
    _batchsize = Integer.parseInt(getProperties().getProperty("batchsize", "100"));
    _buffered = Boolean.parseBoolean(getProperties().getProperty("buffered", "true"));
    _durable = Boolean.parseBoolean(getProperties().getProperty("durable", "true"));
    _client = new HBaseClient(_zkQuorum, _zkBasePath);
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    try {
      _client.shutdown().joinUninterruptibly();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    GetRequest req = new GetRequest(table, key)
            .family(_columnFamilyBytes);
    byte[][] qualifiers = fieldsToQualifiers(fields);
    if (qualifiers != null)
            req = req.qualifiers(qualifiers);

    try {
      if (_debug) {
        System.out.println("Doing read from HBase columnfamily " + _columnFamily);
        System.out.println("Doing read for key: " + key);
      }
      final ArrayList<KeyValue> r = _client.get(req).join();
      for (KeyValue kv : r) {
        result.put(
                bytesToString(kv.qualifier()),
                new ByteArrayByteIterator(kv.value().clone()));
        if (_debug) {
          System.out.println("Result for field: " + bytesToString(kv.qualifier()) +
                  " is: " + bytesToString(kv.value()));
        }
      }
    } catch (Exception e) {
      System.err.println("Error doing get: " + e);
      return ServerError;
    }
    return Ok;
  }

  private byte[][] fieldsToQualifiers(Set<String> fields) {
    if (fields == null)
      return null;
    byte[][] qualifiers = new byte[fields.size()][];
    int idx = 0;
    for (String field : fields) {
      qualifiers[idx++] = Bytes.UTF8(field);
    }
    return qualifiers;
  }

  private String bytesToString(byte[] qualifier) {
    return new String(qualifier, UTF8);
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    final Scanner scanner = _client.newScanner(table);
    try {
      scanner.setStartKey(startkey);
      scanner.setMaxNumRows(recordcount);
      final byte[][] qualifiers;
      if (fields != null) {
        qualifiers = new byte[fields.size()][];
        int idx = 0;
        for (String field : fields) {
          qualifiers[idx++] = Bytes.UTF8(field);
        }
      } else {
        qualifiers = null;
      }
      scanner.setFamilies(new Scanner.ColumnSpec(_columnFamilyBytes, qualifiers));
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (ArrayList<KeyValue> row : rows) {
          if (_debug) {
            System.out.println("Got scan result for key: " + Bytes.pretty(row.get(0).key()));
          }
          HashMap<String, ByteIterator> rowResult = new HashMap<String, ByteIterator>();
          for (KeyValue kv : row) {
            rowResult.put(
                    new String(kv.qualifier(), UTF8),
                    new ByteArrayByteIterator(kv.value().clone()));

          }
          result.add(rowResult);
        }
      }
    } catch (Exception e) {
      if (_debug) {
        System.out.println("Error in getting/parsing scan result: " + e);
      }
      return ServerError;
    } finally {
      scanner.close(); // ? do we need to join here?
    }
    return Ok;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int update(String table, String key, HashMap<String, ByteIterator> values) {

    if (_debug) {
      System.out.println("Setting up put for key: " + key);
    }
    byte[][] qualifiers = new byte[values.size()][];
    byte[][] valuesArr = new byte[values.size()][];
    int idx = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      final byte[] keyBytes = entry.getKey().getBytes(UTF8);
      final byte[] valueBytes = entry.getValue().toArray().clone();
      if (_debug) {
        System.out.println("Adding field/value " + Bytes.pretty(keyBytes) + "/" +
                Bytes.pretty(valueBytes) + " to put request");
      }
      qualifiers[idx] = keyBytes;
      valuesArr[idx] = valueBytes;
      idx++;
    }
    final PutRequest put = new PutRequest(table.getBytes(UTF8), key.getBytes(UTF8), _columnFamilyBytes,
            qualifiers, valuesArr);
    put.setBufferable(_buffered);
    put.setDurable(_durable);
    final Deferred<Object> d = _client.put(put).addErrback(errback);
    if (_failed) {
      return ServerError;
    }
    tryThrottle(d);
    return Ok;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public int delete(String table, String key) {
    if (_debug) {
      System.out.println("Doing delete for key: " + key);
    }

    final Deferred<Object> d = _client.delete(new DeleteRequest(table, key)).addErrback(errback);
    tryThrottle(d);
    return Ok;
  }

  private void tryThrottle(Deferred<Object> d) {
    final int cur = _mutations.incrementAndGet();
    if (cur % _batchsize == 0)
      _client.flush();

    if (_throttle) {
      if (_debug)
        System.out.println("Throttling...");

      long throttle_time = System.nanoTime();
      try {
        d.joinUninterruptibly();
      } catch (Exception e) {
        throw new RuntimeException("Should never happen", e);
      }
      throttle_time = System.nanoTime() - throttle_time;
      if (throttle_time < 1000000000L) {
        if (_debug)
          System.out.println("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted", e);
        }
      }
      if (_debug)
        System.out.println("Done throttling...");
      _throttle = false;
    }
  }

  final class Errback implements Callback<Object, Exception> {
    public Object call(final Exception arg) {
      if (arg instanceof PleaseThrottleException) {
        final PleaseThrottleException e = (PleaseThrottleException) arg;

        if (_debug)
          System.out.println("Need to throttle, HBase isn't keeping up.");

        _throttle = true;
        final HBaseRpc rpc = e.getFailedRpc();
        if (rpc instanceof PutRequest) {
          _client.put((PutRequest) rpc);  // Don't lose edits.
        } else if (rpc instanceof DeleteRequest) {
          _client.delete((DeleteRequest) rpc);  // Don't lose deletes.
        }
        return null;
      }
      _failed = true;
      return arg;
    }

    public String toString() {
      return "errback";
    }
  }

  private final Errback errback = new Errback();

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("usage: ahc zkquorum zkpath threadcount operation_count");
      System.exit(0);
    }

    final int keyspace = 10000; //120000000;
    final String zkQuorum = args[0];
    final String zkPath = args[1];
    final int threadcount = Integer.parseInt(args[2]);
    final int opcount = Integer.parseInt(args[3]) / threadcount;

    Vector<Thread> allthreads = new Vector<Thread>();

    for (int i = 0; i < threadcount; i++) {
      Thread t = new Thread() {
        public void run() {
          try {
            Random random = new Random();

            AsyncHBaseClient cli = new AsyncHBaseClient();

            Properties props = new Properties();
            props.setProperty("columnfamily", "f1");
            props.setProperty("zkquorum", zkQuorum);
            props.setProperty("zkpath", zkPath);
            props.setProperty("debug", "true");
            cli.setProperties(props);

            cli.init();

            HashMap<String, ByteIterator> result = Maps.newHashMap();

            long accum = 0;

            for (int i = 0; i < opcount; i++) {
              int keynum = random.nextInt(keyspace);
              String key = "user" + keynum;
              long st = System.currentTimeMillis();
              int rescode;

              HashMap<String, ByteIterator> hm = Maps.newHashMap();
              hm.put("field1", new ByteArrayByteIterator("value1".getBytes("UTF-8")));
              hm.put("field2", new ByteArrayByteIterator("value2".getBytes("UTF-8")));
              hm.put("field3", new ByteArrayByteIterator("value3".getBytes("UTF-8")));
              hm.put("efield", new ByteArrayByteIterator(HBaseClient.EMPTY_ARRAY));
              rescode = cli.insert("bench", key, hm);
              HashSet<String> s = Sets.newHashSet();
              s.add("field1");
              s.add("field2");

              rescode = cli.read("bench", key, s, result);
              rescode = cli.delete("bench", key);
              rescode = cli.read("bench", key, s, result);

              HashSet<String> scanFields = Sets.newHashSet();
              scanFields.add("field1");
              scanFields.add("field3");
              Vector<HashMap<String, ByteIterator>> scanResults = new Vector<HashMap<String, ByteIterator>>();
              rescode = cli.scan("bench", "user2", 20, null, scanResults);

              long en = System.currentTimeMillis();

              accum += (en - st);

              if (rescode != Ok) {
                System.out.println("Error " + rescode + " for " + key);
              }

              if (i % 10 == 0) {
                System.out.println(i + " operations, average latency: " + (((double) accum) / ((double) i)));
              }
            }

            System.out.println(
                    new ToStringBuilder(cli._client.stats(), ToStringStyle.MULTI_LINE_STYLE).toString()
            );
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      allthreads.add(t);
    }

    long st = System.currentTimeMillis();
    for (Thread t : allthreads) {
      t.start();
    }

    for (Thread t : allthreads) {
      try {
          t.join();
      } catch (InterruptedException e) {
      }
    }
    long en = System.currentTimeMillis();

    System.out.println("Throughput: " + ((1000.0) * (((double) (opcount * threadcount)) / ((double) (en - st)))) + " ops/sec");

  }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
*/

