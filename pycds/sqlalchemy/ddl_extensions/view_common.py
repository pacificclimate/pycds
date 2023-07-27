from sqlalchemy.schema import DDLElement


class ViewCommonDDL(DDLElement):
    """Base class for view and materialized view commands."""

    def __init__(self, name, selectable=None):
        self.name = name
        self.selectable = selectable

