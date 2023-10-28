from __future__ import annotations

from typing import List

from sqlglot import exp, transforms
from sqlglot.dialects.dialect import parse_date_delta, timestamptrunc_sql
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.tsql import generate_date_delta_with_unit_sql
from sqlglot.tokens import TokenType


class Databricks(Spark):
    class Parser(Spark.Parser):
        LOG_DEFAULTS_TO_LN = True
        STRICT_CAST = True

        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
            "DATEADD": parse_date_delta(exp.DateAdd),
            "DATE_ADD": parse_date_delta(exp.DateAdd),
            "DATEDIFF": parse_date_delta(exp.DateDiff),
        }

        FACTOR = {
            **Spark.Parser.FACTOR,
            TokenType.COLON: exp.JSONExtract,
        }

    class Generator(Spark.Generator):
        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.DateAdd: generate_date_delta_with_unit_sql,
            exp.DateDiff: generate_date_delta_with_unit_sql,
            exp.DatetimeAdd: lambda self, e: self.func(
                "TIMESTAMPADD", e.text("unit"), e.expression, e.this
            ),
            exp.DatetimeSub: lambda self, e: self.func(
                "TIMESTAMPADD",
                e.text("unit"),
                exp.Mul(this=e.expression.copy(), expression=exp.Literal.number(-1)),
                e.this,
            ),
            exp.DatetimeDiff: lambda self, e: self.func(
                "TIMESTAMPDIFF", e.text("unit"), e.expression, e.this
            ),
            exp.DatetimeTrunc: timestamptrunc_sql,
            exp.JSONExtract: lambda self, e: self.binary(e, ":"),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.unnest_to_explode,
                ]
            ),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
        }

        TRANSFORMS.pop(exp.TryCast)

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            constraint = expression.find(exp.GeneratedAsIdentityColumnConstraint)
            kind = expression.args.get("kind")
            if (
                constraint
                and isinstance(kind, exp.DataType)
                and kind.this in exp.DataType.INTEGER_TYPES
            ):
                # only BIGINT generated identity constraints are supported
                expression = expression.copy()
                expression.set("kind", exp.DataType.build("bigint"))
            return super().columndef_sql(expression, sep)

        def generatedasidentitycolumnconstraint_sql(
            self, expression: exp.GeneratedAsIdentityColumnConstraint
        ) -> str:
            expression = expression.copy()
            expression.set("this", True)  # trigger ALWAYS in super class
            return super().generatedasidentitycolumnconstraint_sql(expression)

        def merge_sql(self, expression: exp.Merge) -> str:
            # In Databricks SQL's merge into implementation, the statement has to follow the order of: MATCHED clauses, NOT MATCHED [BY TARGET], NOT MATCHED [BY SOURCE]
            # When there are multiple MATCHED clauses, only the last MATCHED clause can OMIT additional conditions
            # When there are multiple NOT MATCHED [BY TARGET] clauses, only the last NOT MATCHED [BY TARGET] clause can OMIT additional conditions
            # When there are multiple NOT MATCHED [BY SOURCE] clauses, only the last NOT MATCHED [BY TARGET] clause can OMIT additional conditions

            sorted_expression = expression.copy()
            matched_clauses_with_conditions: List[int] = []
            matched_clauses_no_conditions: List[int] = []
            not_matched_clauses_by_target_with_conditions: List[int] = []
            not_matched_clauses_by_target_no_conditions: List[int] = []
            not_matched_clauses_by_source_with_conditions: List[int] = []
            not_matched_clauses_by_source_no_conditions: List[int] = []
            sorted_index = [
                matched_clauses_with_conditions,
                matched_clauses_no_conditions,
                not_matched_clauses_by_target_with_conditions,
                not_matched_clauses_by_target_no_conditions,
                not_matched_clauses_by_source_with_conditions,
                not_matched_clauses_by_source_no_conditions,
            ]
            for i in range(len(expression.expressions)):
                exp = expression.expressions[i]
                if exp.args["matched"]:
                    if exp.args["condition"] is None:
                        matched_clauses_no_conditions.append(i)
                    else:
                        matched_clauses_with_conditions.append(i)
                elif exp.args["source"]:
                    if exp.args["condition"] is None:
                        not_matched_clauses_by_source_no_conditions.append(i)
                    else:
                        not_matched_clauses_by_source_with_conditions.append(i)
                else:  # NOT MATCHED BY TARGET
                    if exp.args["condition"] is None:
                        not_matched_clauses_by_target_no_conditions.append(i)
                    else:
                        not_matched_clauses_by_target_with_conditions.append(i)
            curse = 0
            for index_group in sorted_index:
                for index in index_group:
                    sorted_expression.expressions[curse] = expression.expressions[index]
                    curse += 1

            return super().merge_sql(sorted_expression)

    class Tokenizer(Spark.Tokenizer):
        HEX_STRINGS = []

        SINGLE_TOKENS = {
            **Spark.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }
