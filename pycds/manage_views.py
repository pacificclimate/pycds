import logging

from pycds.materialized_view_helpers import MaterializedViewMixin
from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation

daily_views = [DailyMaxTemperature, DailyMinTemperature]
monthly_views = [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, MonthlyTotalPrecipitation]

logger = logging.getLogger(__name__)


def manage_views(session, operation, which_views):

    # Order matters
    views = {
        'daily': daily_views,
        'monthly-only': monthly_views,
        'all': daily_views + monthly_views,
    }[which_views]

    for view in views:
        if operation == 'create' or issubclass(view, MaterializedViewMixin):
            logger.info("{} '{}'".format(operation.capitalize(), view.viewname()))
            getattr(view, operation)(session)
