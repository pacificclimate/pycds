from .content import \
    Thing, Description, SimpleThing, ThingWithDescription, ThingCount


def test_viewname():
    assert SimpleThing.viewname() == 'simple_thing'

def test_simple_view(view_test_session):
    sesh = view_test_session

    things = sesh.query(Thing)
    assert(things.count() == 5)

    view_things = sesh.query(SimpleThing)
    assert [t.id for t in view_things.order_by(SimpleThing.id)] == [1, 2, 3]


def test_complex_view(view_test_session):
    sesh = view_test_session

    things = sesh.query(ThingWithDescription)
    assert [t.desc for t in things.order_by(ThingWithDescription.id)] \
           == ['alpha', 'beta', 'gamma', 'beta', 'alpha']

def test_counts(view_test_session):
    sesh = view_test_session
    counts = sesh.query(ThingCount)
    assert [(c.desc, c.num) for c in counts.order_by(ThingCount.desc)] == \
           [('alpha', 2), ('beta', 2), ('gamma', 1), ]