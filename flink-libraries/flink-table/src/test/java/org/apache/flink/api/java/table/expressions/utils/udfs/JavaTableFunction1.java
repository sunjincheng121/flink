package org.apache.flink.api.java.table.expressions.utils.udfs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.table.functions.TableValuedFunction;
import scala.Array;

import java.util.ArrayList;
import java.util.List;

public class JavaTableFunction1 implements TableValuedFunction<Tuple2<Integer, String>> {
	public Iterable<Tuple2<Integer, String>> eval(String data) {
		List<Tuple2<Integer, String>> result = new ArrayList<Tuple2<Integer, String>>();
		if (data.contains("#")) {
			String[] items = data.split("#");
			result.add(new Tuple2<>(Integer.parseInt(items[0]), items[1]));
		}
		return result;
	}

	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return null;
	}
}
