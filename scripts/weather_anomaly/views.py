"""This module defines the lists of views to be processed by weather anomaly scripts"""

from pycds.weather_anomaly import DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, MonthlyTotalPrecipitation

# In each list, put views in dependency order: for each view, all views on which it depends should appear earlier in
# the list than the view itself. View creation, refreshing, and dropping respect these dependencies.

all_views = [
    DailyMaxTemperature, DailyMinTemperature,
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature,
    MonthlyTotalPrecipitation
]


