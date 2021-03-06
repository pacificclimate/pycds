from ..helpers import get_schema_item_names
from .content import \
    Thing, SimpleThingMatview, ThingWithDescriptionMatview, ThingCountMatview


def test_schema_content(matview_sesh):
    assert get_schema_item_names(matview_sesh, 'tables') >= {
        'things', 'descriptions',
        'simple_thing_matview_mv', 'thing_with_description_matview_mv',
        'thing_count_matview_mv'
    }


def test_viewname(schema_name):
    assert SimpleThingMatview.base_viewname() == 'simple_thing_matview_mv'
    assert SimpleThingMatview.qualfied_viewname() == \
           f'{schema_name}.simple_thing_matview_mv'


def test_simple_view(matview_sesh):
    sesh = matview_sesh

    things = sesh.query(Thing)
    assert things.count() == 5

    # there is nothing in the view before refreshing it
    view_things = sesh.query(SimpleThingMatview)
    assert view_things.count() == 0

    SimpleThingMatview.refresh(sesh)
    assert view_things.count() == 3  # there is something after
    assert [t.id for t in view_things.order_by(SimpleThingMatview.id)] == [1, 2, 3]


def test_complex_view(matview_sesh):
    sesh = matview_sesh

    # there is nothing in the view before refreshing it
    view_things = sesh.query(SimpleThingMatview)
    assert view_things.count() == 0

    ThingWithDescriptionMatview.refresh(sesh)
    things = sesh.query(ThingWithDescriptionMatview)
    assert [t.desc for t in things.order_by(ThingWithDescriptionMatview.id)] \
           == ['alpha', 'beta', 'gamma', 'beta', 'alpha']

def test_counts(matview_sesh):
    sesh = matview_sesh

    # there is nothing in the view before refreshing it
    view_things = sesh.query(SimpleThingMatview)
    assert view_things.count() == 0

    ThingCountMatview.refresh(sesh)
    counts = sesh.query(ThingCountMatview)
    assert [(c.desc, c.num) for c in counts.order_by(ThingCountMatview.desc)] == \
           [('alpha', 2), ('beta', 2), ('gamma', 1), ]
