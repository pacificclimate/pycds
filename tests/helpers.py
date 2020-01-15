"""Helpers for tests."""


# Fixture helpers

# The following generators abstract behavior common to many fixtures in this
# test suite. The behaviour pattern is:
#
#   def behaviour(session, ...)
#       setup(session, ...)
#       yield session
#       teardown(session, ...)
#
# Two examples of this pattern are
#
#   setup = add database objects to session
#   teardown = remove database objects from session
#
# and
#
#   setup = create views in session
#   teardown = drop views in session
#
# To use such a generator correctly, i.e., so that the teardown after the
# yield is also performed, a fixture must first yield the result of
# `next(behaviour)`, then call `next(behaviour)` again. This can be done
# in two ways:
#
#   g = behaviour(...)
#   yield next(g)
#   next(g)
#
# or, shorter and clearer:
#
#   for sesh in behaviour(...):
#       yield sesh
#
# The shorter method is preferred.

def generic_sesh(sesh, sa_objects):
    """Add objects to session, yield session, drop objects from session (in
    reverse order. For correct usage, see notes above.

    Args:
        sesh (sqlalchemy.orm.session.Session): database session

        sa_objects: List of SQLAlchemy ORM objects to be added to database
            for setup and removed on teardown. Order within list is respected
            for setup and teardown, so that dependencies are respected.

    Returns:
        yields sesh after setup
    """
    for sao in sa_objects:
        sesh.add(sao)
        sesh.flush()
    yield sesh
    for sao in reversed(sa_objects):
        sesh.delete(sao)
        sesh.flush()


def create_then_drop_views(sesh, views):
    """Create views in session, yield session, drop views in session (in
    reverse order). For correct usage, see notes above.

    Args:
        sesh (sqlalchemy.orm.session.Session): database session

        views: List of views created in database on setup and dropped
            on teardown. Order within list is respected for setup and
            teardown, so that dependencies can be respected.

    Returns:
        yields sesh after setup

    """
    for view in views:
        view.create(sesh)
    yield sesh
    for view in reversed(views):
        view.drop(sesh)
