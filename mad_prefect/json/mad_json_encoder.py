import datetime
import decimal
import json
import uuid


class MADJSONEncoder(json.JSONEncoder):
    def default(self, data):
        if isinstance(data, decimal.Decimal):
            return float(data)

        if isinstance(data, datetime.datetime):
            # DuckDB doesn't recognize the ISO 8601 timestamp format with 'T' by default.
            # DuckDB's automatic type inference for timestamps expects a space ' ' between the date and time components.
            return data.isoformat(
                " ", "microseconds"
            )  # Always serialize microseconds as duckdb does not support a variable date format

        if isinstance(data, datetime.time):
            return data.isoformat()

        if isinstance(data, datetime.date):
            return data.isoformat()

        if isinstance(data, uuid.UUID):
            return str(data)

        return super().default(data)
