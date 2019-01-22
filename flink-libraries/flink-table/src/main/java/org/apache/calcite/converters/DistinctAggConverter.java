package org.apache.calcite.converters;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.DistinctAgg;

public class DistinctAggConverter {

    public static RelBuilder.AggCall toAggCall (
            DistinctAgg agg, String name, RelBuilder relBuilder) {
        Aggregation child = (Aggregation) agg.child();
        try {
            return (RelBuilder.AggCall)AggregationConverterUtil.class.getDeclaredMethod
                    ("toAggCall", child.getClass(),
                     String.class, boolean.class,
                     RelBuilder.class).invoke(null, child, name, true, relBuilder);
        } catch (NoSuchMethodException e) {
            throw new TableException("xxx");
        }catch (Exception e){
            throw new TableException("xxx");
        }
    }

    public static SqlAggFunction getSqlAggFunction(DistinctAgg agg, RelBuilder relBuilder) {
        Aggregation child = (Aggregation) agg.child();
        try {
            return (SqlAggFunction)AggregationConverterUtil.class.getDeclaredMethod
                    ("getSqlAggFunction", child.getClass(),
                     RelBuilder.class).invoke(null, child, relBuilder);
        } catch (NoSuchMethodException e) {
            throw new TableException("xxx");
        }catch (Exception e){
            throw new TableException("xxx");
        }
    }
}
