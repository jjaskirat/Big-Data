/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import org.apache.hadoop.io.WritableUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import java.io.*;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private MapFile.Reader[] index;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private int numReducers;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
    FileStatus[] status = fs.listStatus(new Path(indexPath));
    numReducers = status.length - 1;
    index = new MapFile.Reader[status.length];

    for (int i=0; i < status.length; i++) {
      if (status[i].getPath().toString().contains("SUCCESS")) continue;
      index[i] = new MapFile.Reader(new Path("" + status[i].getPath()), fs.getConf());
    }

    collection = fs.open(new Path(collectionPath));
    stack = new Stack<Set<Integer>>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<Integer>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    Text key = new Text();
    BytesWritable value = new BytesWritable();

    key.set(term);
    int i = (term.hashCode() & Integer.MAX_VALUE) % numReducers;
    index[i+1].get(key, value);
    
    byte[] MyBytes = value.getBytes();
    ByteArrayInputStream bis = new ByteArrayInputStream(MyBytes);
    DataInputStream postings = new DataInputStream(bis);
    ArrayListWritable<PairOfInts> MyPair = new ArrayListWritable<PairOfInts>();

    int docNo = 0;
    int df = WritableUtils.readVInt(postings);

    for(i = 0; i < df; i++){
      int docNoDiff = WritableUtils.readVInt(postings);
      int tf = WritableUtils.readVInt(postings);
      docNo += docNoDiff;
      MyPair.add(new PairOfInts(docNo, tf));
    }

    return MyPair;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  public static class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    public String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    public String query;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
