package org.apache.flink.api.java.batch.sql;

import org.apache.flink.api.table.functions.TableValuedFunction;

import java.util.ArrayList;
import java.util.List;

public class SplitTVF extends TableValuedFunction<String> {
	public java.lang.Iterable<String> eval(String str) {
		List<String> rows = new ArrayList<String>();
		if (str.contains("#")) {
			String[] items = str.split("#");
			for (String item : items) {
				rows.add(item);
			}
		}
		return rows;
	}
}
