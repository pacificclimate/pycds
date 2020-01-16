from .content import \
    Thing, SimpleThingView, ThingWithDescriptionView, ThingCountView


def test_viewname():
    assert SimpleThingView.viewname() == 'simple_thing_view'


def test_simple_view(view_sesh):
    sesh = view_sesh

    things = sesh.query(Thing)
    assert(things.count() == 5)

    view_things = sesh.query(SimpleThingView)
    assert [t.id for t in view_things.order_by(SimpleThingView.id)] == [1, 2, 3]


def test_complex_view(view_sesh):
    sesh = view_sesh

    things = sesh.query(ThingWithDescriptionView)
    assert [t.desc for t in things.order_by(ThingWithDescriptionView.id)] \
           == ['alpha', 'beta', 'gamma', 'beta', 'alpha']


def test_counts(view_sesh):
    sesh = view_sesh
    counts = sesh.query(ThingCountView)
    assert [(c.desc, c.num) for c in counts.order_by(ThingCountView.desc)] == \
           [('alpha', 2), ('beta', 2), ('gamma', 1), ]