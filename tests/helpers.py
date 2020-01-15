"""Helpers for tests."""


# Fixture helpers

def create_then_drop_views(sesh, views):
    """Create views in session, yield session, drop views in session (in
    reverse order).

    Args:
        sesh (sqlalchemy.orm.session.Session): database session

        views: List of views created in database on setup and dropped
            on teardown. Order within iterable is respected for setup and
            teardown, so that dependencies can be respected.

    Returns:
        yields sesh after setup

    To use this generator correctly, i.e., so that the teardown after the
    yield is also performed, a fixture must first yield the result of next(g),
    then call next(g) again. This can be done two ways:

      g = create_then_drop_views(...)
      yield next(g)
      next(g)

    or, shorter and clearer:

      for sesh in create_then_drop_views(...):
          yield sesh

    The shorter method is preferred.
    """
    for view in views:
        view.create(sesh)
    yield sesh
    for view in reversed(views):
        view.drop(sesh)
