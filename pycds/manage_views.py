import logging

from pycds.materialized_view_helpers import MaterializedViewMixin
from pycds.weather_anomaly import \
    DiscardedObs, \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation

base_views = [DiscardedObs]
daily_views = [DailyMaxTemperature, DailyMinTemperature]
monthly_views = [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, MonthlyTotalPrecipitation]

logger = logging.getLogger(__name__)


def manage_views(session, operation, which_views):

    # Order matters
    views = {
        'base': base_views,
        'daily': base_views + daily_views,
        'monthly': base_views + monthly_views,
        'all': base_views + daily_views + monthly_views,
        'base-only': base_views,
        'daily-only': daily_views,
        'monthly-only': monthly_views,
    }[which_views]

    for view in views:
        if operation == 'create' or issubclass(view, MaterializedViewMixin):
            logger.info("{} '{}'".format(operation.capitalize(), view.viewname()))
            getattr(view, operation)(session)
