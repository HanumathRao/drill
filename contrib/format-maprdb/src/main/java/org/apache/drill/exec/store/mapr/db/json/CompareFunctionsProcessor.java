/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.mapr.db.json;

import static com.mapr.db.rowcol.DBValueBuilderImpl.KeyValueBuilder;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.Decimal28Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal38Expression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.ValueExpressions.VarDecimalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.joda.time.LocalTime;
import org.ojai.Value;
import org.ojai.types.ODate;
import org.ojai.types.OTime;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import com.mapr.db.util.SqlHelper;

import org.ojai.types.OTimestamp;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

class CompareFunctionsProcessor extends AbstractExprVisitor<Boolean, List<LogicalExpression>, RuntimeException> {

  private String functionName;
  private Boolean success;
  protected List<Value> values;
  protected SchemaPath path;

  public CompareFunctionsProcessor(String functionName) {
    this.functionName = functionName;
    this.success = false;
    this.values = new ArrayList<>();
  }

  public static boolean isCompareFunction(String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, List<LogicalExpression> valueArg) throws RuntimeException {
    return false;
  }

  /**
   * Converts specified function call to be pushed into maprDB JSON scan.
   *
   * @param call function call to be pushed
   * @return CompareFunctionsProcessor instance which contains converted function call
   */
  public static CompareFunctionsProcessor process(FunctionCall call) {
    return processWithEvaluator(call, new CompareFunctionsProcessor(call.getName()));
  }

  /**
   * Converts specified function call to be pushed into maprDB JSON scan.
   * For the case when timestamp value is used, it is converted to UTC timezone
   * before converting to {@link OTimestamp} instance.
   *
   * @param call function call to be pushed
   * @return CompareFunctionsProcessor instance which contains converted function call
   */
  public static CompareFunctionsProcessor processWithTimeZoneOffset(FunctionCall call) {
    CompareFunctionsProcessor processor = new CompareFunctionsProcessor(call.getName()) {
      @Override
      protected boolean visitTimestampExpr(SchemaPath path, TimeStampExpression valueArg) {
        // converts timestamp value from local time zone to UTC since the record reader
        // reads the timestamp in local timezone if the readTimestampWithZoneOffset flag is enabled
        Instant localInstant = Instant.ofEpochMilli(valueArg.getTimeStamp());
        ZonedDateTime utcZonedDateTime = localInstant.atZone(ZoneId.of("UTC"));
        ZonedDateTime convertedZonedDateTime = utcZonedDateTime.withZoneSameLocal(ZoneId.systemDefault());
        long timeStamp = convertedZonedDateTime.toInstant().toEpochMilli();

        this.values.add(KeyValueBuilder.initFrom(new OTimestamp(timeStamp)));
        this.path = path;
        return true;
      }
    };
    return processWithEvaluator(call, processor);
  }

  private static CompareFunctionsProcessor processWithEvaluator(FunctionCall call, CompareFunctionsProcessor evaluator) {
    String functionName = call.getName();
    LogicalExpression nameArg = call.args.get(0);
    List<LogicalExpression> valueArgs = new ArrayList<>();
    for (int i=1;i<call.args.size();i++) {
      valueArgs.add(call.args.get(i));
    }
    if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
      LogicalExpression swapArg = nameArg;
      nameArg = valueArgs.get(0);
      valueArgs.clear();
      valueArgs.add(swapArg);
      evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
    }
    if (nameArg != null) {
      evaluator.success = nameArg.accept(evaluator, valueArgs);
    }

    return evaluator;
  }

  public boolean isSuccess() {
    // TODO Auto-generated method stub
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  public Value getValue(int index) {
    return index < values.size() ? values.get(index) : null;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, List<LogicalExpression> valueArgs) throws RuntimeException {
    // If valueArg is null, this might be a IS NULL/IS NOT NULL type of query
    this.path = path;
    if (valueArgs.size() == 0) {
      return true;
    }

    for (LogicalExpression valueArg : valueArgs) {

      if (valueArg instanceof QuotedString) {
        this.values.add(SqlHelper.decodeStringAsValue(((QuotedString) valueArg).value));
      } else if (valueArg instanceof IntExpression) {
        this.values.add(KeyValueBuilder.initFrom(((IntExpression) valueArg).getInt()));
      } else if (valueArg instanceof FloatExpression) {
        this.values.add(KeyValueBuilder.initFrom(((FloatExpression) valueArg).getFloat()));
      } else if (valueArg instanceof BooleanExpression) {
        this.values.add(KeyValueBuilder.initFrom(((BooleanExpression) valueArg).getBoolean()));
      } else if (valueArg instanceof Decimal28Expression) {
        this.values.add(KeyValueBuilder.initFrom(((Decimal28Expression) valueArg).getBigDecimal()));
      } else if (valueArg instanceof Decimal38Expression) {
        this.values.add(KeyValueBuilder.initFrom(((Decimal38Expression) valueArg).getBigDecimal()));
      } else if (valueArg instanceof DoubleExpression) {
        this.values.add(KeyValueBuilder.initFrom(((DoubleExpression) valueArg).getDouble()));
      } else if (valueArg instanceof LongExpression) {
        this.values.add(KeyValueBuilder.initFrom(((LongExpression) valueArg).getLong()));
      } else if (valueArg instanceof DateExpression) {
        long d = ((DateExpression) valueArg).getDate();
        final long MILLISECONDS_IN_A_DAY = (long) 1000 * 60 * 60 * 24;
        int daysSinceEpoch = (int) (d / MILLISECONDS_IN_A_DAY);
        this.values.add(KeyValueBuilder.initFrom(ODate.fromDaysSinceEpoch(daysSinceEpoch)));
      } else if (valueArg instanceof TimeExpression) {
        int t = ((TimeExpression) valueArg).getTime();
        LocalTime lT = LocalTime.fromMillisOfDay(t);
        this.values.add(KeyValueBuilder.initFrom(new OTime(lT.getHourOfDay(), lT.getMinuteOfHour(), lT.getSecondOfMinute(), lT.getMillisOfSecond())));
      } else if (valueArg instanceof VarDecimalExpression) {
        // MaprDB does not support decimals completely, therefore double value is used.
        // See com.mapr.db.impl.ConditionImpl.is(FieldPath path, QueryCondition.Op op, BigDecimal value) method
        this.values.add(KeyValueBuilder.initFrom(((VarDecimalExpression) valueArg).getBigDecimal().doubleValue()));
      } else if (valueArg instanceof TimeStampExpression) {
        return visitTimestampExpr(path, (TimeStampExpression) valueArg);
      }
    }
    return values.size() > 0;
  }

  protected boolean visitTimestampExpr(SchemaPath path, TimeStampExpression valueArg) {
    this.values.add(KeyValueBuilder.initFrom(new OTimestamp(valueArg.getTimeStamp())));
    this.path = path;
    return true;
  }

  private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
  static {
    ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
    VALUE_EXPRESSION_CLASSES = builder
        .add(BooleanExpression.class)
        .add(DateExpression.class)
        .add(DoubleExpression.class)
        .add(FloatExpression.class)
        .add(IntExpression.class)
        .add(LongExpression.class)
        .add(QuotedString.class)
        .add(TimeExpression.class)
        .add(VarDecimalExpression.class)
        .build();
  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
     // unary functions
        .put("isnotnull", "isnotnull")
        .put("isNotNull", "isNotNull")
        .put("is not null", "is not null")
        .put("isnull", "isnull")
        .put("isNull", "isNull")
        .put("is null", "is null")
        // binary functions
        .put("like", "like")
        .put("equal", "equal")
        .put("not_equal", "not_equal")
        .put("greater_than_or_equal_to", "less_than_or_equal_to")
        .put("greater_than", "less_than")
        .put("less_than_or_equal_to", "greater_than_or_equal_to")
        .put("less_than", "greater_than")
        .build();
  }

}
