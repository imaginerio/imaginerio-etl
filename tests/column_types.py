import os
from dagster_pandas.constraints import ColumnConstraint, ColumnConstraintViolationException
from tests.objects_types import *
import pandas as pd
import dagster as dg
import dagster_pandas as dp
import validators
import datetime
import geojson
import json


class url_column(ColumnConstraint):
    def __init__(self):
        message = "Value must be a  valid link"
        super(url_column, self).__init__(
            error_description=message, markdown_description=message
        )

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: False if validators.url(x.split(" ")[0]) else True)]
        if not rows_with_unexpected_buckets.empty:
            raise ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )


class str_column(ColumnConstraint):
    def __init__(self):
        message = "Value must be a string"
        super(str_column, self).__init__(
            error_description=message, markdown_description=message
        )

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: False if isinstance(x, str) else True)]
        if not rows_with_unexpected_buckets.empty:
            raise ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )


class str_unique_column(ColumnConstraint):
    def __init__(self):
        message = "Value must be a string"
        super(str_column, self).__init__(
            error_description=message, markdown_description=message
        )

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: False if isinstance(x, str) else True)]
        if not rows_with_unexpected_buckets.empty:
            raise ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )


class int_column(ColumnConstraint):
    def __init__(self):
        message = "Value must be a integer"
        super(int_column, self).__init__(
            error_description=message, markdown_description=message
        )

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: False if isinstance(x, int) else True)]
        if not rows_with_unexpected_buckets.empty:
            raise ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )


class float_column(ColumnConstraint):
    def __init__(self):
        message = "Value must be a float"
        super(float_column, self).__init__(
            error_description=message, markdown_description=message
        )

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: False if isinstance(x, float) else True)]
        if not rows_with_unexpected_buckets.empty:
            raise ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )
