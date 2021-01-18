from ..helpers import get_schema_item_names
from .content import (
    Thing,
    SimpleThingManualMatview,
    ThingWithDescriptionManualMatview,
    ThingCountManualMatview
)


def test_schema_content(manual_matview_sesh):
    assert get_schema_item_names(manual_matview_sesh, 'tables') >= {
        'things',
        'descriptions',
        'simple_thing_manual_matview_mv',
        'thing_with_description_manual_matview_mv',
        'thing_count_manual_matview_mv'
    }


def test_viewname(schema_name):
    assert (
        SimpleThingManualMatview.base_viewname() ==
        'simple_thing_manual_matview_mv'
    )
    assert (
        SimpleThingManualMatview.qualfied_viewname() ==
        f'{schema_name}.simple_thing_manual_matview_mv'
    )


def test_simple_view(manual_matview_sesh):
    sesh = manual_matview_sesh

    things = sesh.query(Thing)
    assert things.count() == 5

    # there is nothing in the view before refreshing it
    view_things = sesh.query(SimpleThingManualMatview)
    assert view_things.count() == 0

    SimpleThingManualMatview.refresh(sesh)
    assert view_things.count() == 3  # there is something after
    assert (
        [t.id for t in view_things.order_by(SimpleThingManualMatview.id)]
        == [1, 2, 3]
    )


def test_complex_view(manual_matview_sesh):
    sesh = manual_matview_sesh

    # there is nothing in the view before refreshing it
    view_things = sesh.query(SimpleThingManualMatview)
    assert view_things.count() == 0

    ThingWithDescriptionManualMatview.refresh(sesh)
    things = sesh.query(ThingWithDescriptionManualMatview)
    assert (
        [t.desc for t in things.order_by(ThingWithDescriptionManualMatview.id)]
        == ['alpha', 'beta', 'gamma', 'beta', 'alpha']
    )

def test_counts(manual_matview_sesh):
    sesh = manual_matview_sesh

    # there is nothing in the view before refreshing it
    view_things = sesh.query(SimpleThingManualMatview)
    assert view_things.count() == 0

    ThingCountManualMatview.refresh(sesh)
    counts = sesh.query(ThingCountManualMatview)
    assert (
        [(c.desc, c.num) for c in counts.order_by(ThingCountManualMatview.desc)]
        == [('alpha', 2), ('beta', 2), ('gamma', 1), ]
    )
