/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.batch.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.table.expressions.utils.udfs.JavaTableFunction1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.flink.api.table.expressions.utils.TableValuedFunction0;

import java.util.ArrayList;

import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class SqlITCase extends TableProgramsTestBase {

	public SqlITCase(TestExecutionMode mode, TableConfigMode configMode) {
		super(mode, configMode);
	}

	@Test
	public void testSelectFromTable() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(ds, "a,b,c");
		tableEnv.registerTable("T", in);

		String sqlQuery = "SELECT a, c FROM T";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,Hi\n" + "2,Hello\n" + "3,Hello world\n" +
				"4,Hello world, how are you?\n" + "5,I am fine.\n" + "6,Luke Skywalker\n" +
				"7,Comment#1\n" + "8,Comment#2\n" + "9,Comment#3\n" + "10,Comment#4\n" +
				"11,Comment#5\n" + "12,Comment#6\n" + "13,Comment#7\n" +
				"14,Comment#8\n" + "15,Comment#9\n" + "16,Comment#10\n" +
				"17,Comment#11\n" + "18,Comment#12\n" + "19,Comment#13\n" +
				"20,Comment#14\n" + "21,Comment#15\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testFilterFromDataSet() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet("DataSetTable", ds, "x, y, z");

		String sqlQuery = "SELECT x FROM DataSetTable WHERE z LIKE '%Hello%'";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "2\n" + "3\n" + "4";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet("AggTable", ds, "x, y, z");

		String sqlQuery = "SELECT sum(x), min(x), max(x), count(y), avg(x) FROM AggTable";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "231,1,21,21,11";
		compareResultAsText(results, expected);
	}

	@Test
	public void testJoin() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		tableEnv.registerDataSet("t1", ds1, "a, b, c");
		tableEnv.registerDataSet("t2", ds2, "d, e, f, g, h");

		String sqlQuery = "SELECT c, g FROM t1, t2 WHERE b = e";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testUDTF() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(ds, "a,b,c");
		tableEnv.registerTable("MyTable", in);
		tableEnv.registerFunction("split", new TableValuedFunction0());
		String sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.s FROM " +
				"MyTable,LATERAL TABLE(split(c)) AS t(s)";
		Table result = tableEnv.sql(sqlQuery);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "1,6,Hi\n1,6,KEVIN\n2,7,Hello\n2,7,SUNNY\n4,8,LOVER\n4,8,PAN";
		compareResultAsText(results, expected);
	}

	@Test
	public void testUDTFWithFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(ds, "a,b,c");
		tableEnv.registerTable("MyTable", in);
		tableEnv.registerFunction("split", new TableValuedFunction0());
		String sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.s FROM " +
				"MyTable,LATERAL TABLE(split(c)) AS t(s) WHERE MyTable.a <4";
		Table result = tableEnv.sql(sqlQuery);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "1,6,Hi\n1,6,KEVIN\n2,7,Hello\n2,7,SUNNY";
		compareResultAsText(results, expected);
	}

	@Test
	public void testLeftJoinUDTF() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(ds, "a,b,c");
		tableEnv.registerTable("MyTable", in);
		tableEnv.registerFunction("split", new TableValuedFunction0());
		String sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.s FROM " +
				"MyTable LEFT JOIN LATERAL TABLE(split(c)) AS t(s) ON TRUE";
		Table result = tableEnv.sql(sqlQuery);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "1,6,Hi\n1,6,KEVIN\n2,7,Hello\n2,7,SUNNY\n3,7,null\n4,8,LOVER\n4,8,PAN";
		compareResultAsText(results, expected);
	}

	@Test
	public void testInnerJoinUDTF() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet2(env);
		Table in = tableEnv.fromDataSet(ds, "a,b,c");
		tableEnv.registerTable("MyTable", in);
		tableEnv.registerFunction("split", new JavaTableFunction1());
		String sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.age,t.name FROM MyTable"+
				" JOIN LATERAL TABLE(split(c)) AS t(age,name) ON MyTable.a = t.age ";
		Table result = tableEnv.sql(sqlQuery);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		System.out.println(results);
		String expected = "1,6,1,KEVIN\n2,7,2,SUNNY";
		compareResultAsText(results, expected);
	}

	public static DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet2(ExecutionEnvironment env) {

		List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
		data.add(new Tuple3<>(1, 1L, "1#KEVIN"));
		data.add(new Tuple3<>(2, 2L, "2#SUNNY"));
		data.add(new Tuple3<>(3, 2L, "Hello world"));
		data.add(new Tuple3<>(4, 3L, "20#LOVER"));
		Collections.shuffle(data);

		return env.fromCollection(data);
	}
	public static DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet(ExecutionEnvironment env) {

		List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
		data.add(new Tuple3<>(1, 1L, "Hi#KEVIN"));
		data.add(new Tuple3<>(2, 2L, "Hello#SUNNY"));
		data.add(new Tuple3<>(3, 2L, "Hello world"));
		data.add(new Tuple3<>(4, 3L, "PAN#LOVER"));
		Collections.shuffle(data);

		return env.fromCollection(data);
	}


}

