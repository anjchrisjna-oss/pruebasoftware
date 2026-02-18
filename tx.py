from contextlib import contextmanager


@contextmanager
def smart_begin(db):
    """Begin a transaction safely.

    SQLAlchemy sessions often start an implicit transaction after the first
    query/flush. If we try to call `db.begin()` again, SQLAlchemy raises:
      InvalidRequestError: A transaction is already begun on this Session.

    This helper opens a normal transaction when none is active, or a nested
    transaction (SAVEPOINT) when one is already active.
    """
    if db.in_transaction():
        with db.begin_nested():
            yield
    else:
        with db.begin():
            yield
