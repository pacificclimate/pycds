import pytest
from ..helpers import get_schema_item_names
from .content import (
    Thing,
    SimpleThingManualMatview,
    ThingWithDescriptionManualMatview,
    ThingCountManualMatview,
    SimpleThingNativeMatview,
    ThingWithDescriptionNativeMatview,
    ThingCountNativeMatview
)


def test_manual_matview_schema_content(manual_matview_sesh):
    assert get_schema_item_names(manual_matview_sesh, 'tables') >= {
        'things',
        'descriptions',
        'simple_thing_manual_matview_mv',
        'thing_with_description_manual_matview_mv',
        'thing_count_manual_matview_mv'
    }


def test_native_matview_schema_content(native_matview_sesh):
    assert get_schema_item_names(native_matview_sesh, 'tables') >= {
        'things', 'descriptions',
    }
    assert get_schema_item_names(native_matview_sesh, 'matviews') >= {
        'simple_thing_native_matview_mv',
        'thing_with_description_native_matview_mv',
        'thing_count_native_matview_mv'
    }


@pytest.mark.parametrize(
    "View, viewname",
    [
        (SimpleThingManualMatview, "simple_thing_manual_matview_mv"),
        (SimpleThingNativeMatview, "simple_thing_native_matview_mv"),
    ]
)
def test_viewname(schema_name, View, viewname):
    assert View.base_viewname() == viewname
    assert View.qualfied_viewname() == f'{schema_name}.{viewname}'


def check_simple_matview(sesh, SimpleThingMatview):
    things = sesh.query(Thing)
    assert things.count() == 5

    # there is nothing in the view before refreshing it
    view_things = sesh.query(SimpleThingMatview)
    assert view_things.count() == 0

    SimpleThingMatview.refresh(sesh)
    assert view_things.count() == 3  # there is something after
    assert (
        [t.id for t in view_things.order_by(SimpleThingMatview.id)]
        == [1, 2, 3]
    )


def test_simple_manual_matview(manual_matview_sesh):
    check_simple_matview(manual_matview_sesh, SimpleThingManualMatview)


def test_simple_native_matview(native_matview_sesh):
    check_simple_matview(native_matview_sesh, SimpleThingNativeMatview)


def check_complex_matview(sesh, ThingWithDescriptionMatview):
    # there is nothing in the view before refreshing it
    view_things = sesh.query(ThingWithDescriptionMatview)
    assert view_things.count() == 0

    ThingWithDescriptionMatview.refresh(sesh)
    things = sesh.query(ThingWithDescriptionMatview)
    assert (
        [t.desc for t in things.order_by(ThingWithDescriptionMatview.id)]
        == ['alpha', 'beta', 'gamma', 'beta', 'alpha']
    )


def test_complex_manual_matview(manual_matview_sesh):
    check_complex_matview(
        manual_matview_sesh, ThingWithDescriptionManualMatview
    )


def test_complex_native_matview(native_matview_sesh):
    check_complex_matview(
        native_matview_sesh, ThingWithDescriptionNativeMatview
    )


def check_counts_matview(sesh, ThingCountMatview):
    # there is nothing in the view before refreshing it
    view_things = sesh.query(ThingCountMatview)
    assert view_things.count() == 0

    ThingCountMatview.refresh(sesh)
    counts = sesh.query(ThingCountMatview)
    assert (
        [(c.desc, c.num) for c in counts.order_by(ThingCountMatview.desc)]
        == [('alpha', 2), ('beta', 2), ('gamma', 1), ]
    )


def test_counts_manual_matview(manual_matview_sesh):
    check_counts_matview(manual_matview_sesh, ThingCountManualMatview)


def test_counts_native_matview(native_matview_sesh):
    check_counts_matview(native_matview_sesh, ThingCountNativeMatview)
