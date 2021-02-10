from tests.helpers import get_schema_item_names
from .content import (
    Thing,
    SimpleThingView,
    ThingWithDescriptionView,
    ThingCountView,
)


def test_schema_content(view_sesh):
    assert get_schema_item_names(view_sesh, "tables") >= {
        "things",
        "descriptions",
    }
    assert get_schema_item_names(view_sesh, "views") >= {
        "simple_thing_v",
        "thing_with_description_v",
        "thing_count_v",
    }


def test_viewname(schema_name):
    assert SimpleThingView.base_name() == "simple_thing_v"
    assert SimpleThingView.qualified_name() == f"{schema_name}.simple_thing_v"


def test_simple_view(view_sesh):
    sesh = view_sesh

    things = sesh.query(Thing)
    assert things.count() == 5

    view_things = sesh.query(SimpleThingView)
    assert [t.id for t in view_things.order_by(SimpleThingView.id)] == [1, 2, 3]


def test_complex_view(view_sesh):
    sesh = view_sesh
    things = sesh.query(ThingWithDescriptionView)
    assert [t.desc for t in things.order_by(ThingWithDescriptionView.id)] == [
        "alpha",
        "beta",
        "gamma",
        "beta",
        "alpha",
    ]


def test_counts(view_sesh):
    sesh = view_sesh
    counts = sesh.query(ThingCountView)
    assert [(c.desc, c.num) for c in counts.order_by(ThingCountView.desc)] == [
        ("alpha", 2),
        ("beta", 2),
        ("gamma", 1),
    ]
