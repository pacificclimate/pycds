import logging

from pycds.materialized_view_helpers import MaterializedViewMixin
from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation

daily_views = [DailyMaxTemperature, DailyMinTemperature]
monthly_views = [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, MonthlyTotalPrecipitation]

logger = logging.getLogger(__name__)


def manage_views(session, operation, which_set):
    """Apply specified view management operation to specified set of views.

    :param session: (sqlalchemy.orm.session.Session) database session
    :param operation: (str) operation to apply, one of 'create', 'refresh' (actually the name of any valid method
        of a materialized view can be supplied here, but the invoking script limits it to above list).
    :param which_set: (str) which set of views to apply the operation to, one of 'daily', 'monthly-only', 'all'
    """

    views = {
        'daily': daily_views,
        'monthly-only': monthly_views,
        'all': daily_views + monthly_views, # Order of view updating matters
    }[which_set]

    for view in views:
        if operation == 'create' or issubclass(view, MaterializedViewMixin):
            logger.info("{} '{}'".format(operation.capitalize(), view.viewname()))
            getattr(view, operation)(session)
