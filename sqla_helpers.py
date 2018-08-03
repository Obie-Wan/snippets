@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    session.begin()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def resolve_transaction_conflicts(retries=SQL_MAX_CONFLICT_RETRIES, logger=None):
    """SQL transaction conflict resolution.

    ``resolve_transaction_conflicts`` decorator will retry to run everything inside the function
    May be used with mappers and managers

    Usage:

    SomeManager(BaseManager):
    ...
        @managed_transaction
        def myfunc(self):
            # Both threads modify the same object simultaneously
            obj = self._mapper.query(Obj).get(1)
            obj.param += 1

    # Execute the conflict sensitive code inside a managed transaction
    manager.myfunc()

    The rules:

    - You must not swallow all exceptions within ``managed_transaction``
    - Use read-only database sessions if you know you do not need to modify the database
    and you need weaker transaction guarantees e.g. for displaying current object state.
    - Never do external actions inside ``managed_transaction``.
    If the database transaction is replayed, the code is run twice
    - Managed transaction section should be as small and fast as possible
    - Avoid long-running transactions by splitting up big transaction to smaller worker batches
    This implementation is based on simplified & adapted code by Mikko Ohtamaa
    """

    def _managed_transaction(func):
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            attempts = 0

            if hasattr(self, '_session'):
                # use in mapper
                session = self._session
            elif hasattr(self, '_mapper'):
                # use in manager
                session = self._mapper.session
            else:
                raise RuntimeError('Invalid decorator usage')

            while attempts <= retries:
                try:
                    return func(self, *args, **kwargs)

                except OperationalError as exc:
                    if is_retryable_exception(exc):
                        session.close()

                        attempts += 1
                        if attempts > retries:
                            raise UnresolvedDatabaseConflict(
                                "Could not replay the transaction {} after {} attempts".format(
                                    func, retries)) from exc

                        time.sleep(0.1)
                        if logger:
                            logger.debug('[%i] Retrying transaction - db conflict occurred', attempts)

                        session.begin()
                        continue
                    else:
                        session.rollback()
                        session.begin()
                        raise

                except Exception:
                    session.rollback()
                    session.begin()
                    raise

        return wrapped

    return _managed_transaction

