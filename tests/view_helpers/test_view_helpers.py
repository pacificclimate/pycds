from ..helpers import get_items_in_schema
from .content import \
    Thing, SimpleThingView, ThingWithDescriptionView, ThingCountView


def test_schema_content(view_sesh):
    assert get_items_in_schema(view_sesh, 'tables') >= {
        'things', 'descriptions'
    }
    # Wrong
    # assert get_items_in_schema(view_sesh, 'views', schema_name='public') >= {
    #     'simple_thing_view', 'thing_with_description_view',
    #     'thing_count_view'
    # }
    assert get_items_in_schema(view_sesh, 'views') >= {
        'simple_thing_view', 'thing_with_description_view',
        'thing_count_view'
    }


def test_viewname(schema_name):
    assert SimpleThingView.base_viewname() == 'simple_thing_view'
    assert SimpleThingView.qualfied_viewname() == \
           f'{schema_name}.simple_thing_view'


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