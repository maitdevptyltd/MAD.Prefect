import os


CONNECTION_STRING = os.getenv("CONNECTION_STRING")


def get_engine(connection_string: str | None = None):
    import sqlalchemy

    return sqlalchemy.create_engine(connection_string or CONNECTION_STRING)
