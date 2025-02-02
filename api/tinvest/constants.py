from datetime import date, time, timedelta
from api.tinvest.datatypes import (InstrumentType)
from engine.schemas.datatypes import ExchangeIntervalTree
from engine.schemas.enums import SessionPeriod

start_date = date(year=2018, month=3, day=9)

# time of the start of the candle (working day is defined as open <= t < close)
working_hours = ExchangeIntervalTree(
    {InstrumentType.STOCK:
         {start_date: {'start': time(hour=13, minute=59),
                       'duration': timedelta(hours=10, minutes=1)},
          date(year=2018, month=3, day=13): {'start': time(hour=6, minute=59),
                                             'duration': timedelta(hours=17, minutes=1)},
          date(year=2018, month=6, day=8): {'start': time(hour=6, minute=59),
                                            'duration': timedelta(hours=8, minutes=47)},  # 15:46 is closing time
          date(year=2020, month=6, day=22): {'start': time(hour=6, minute=59),
                                             'duration': timedelta(hours=13, minutes=51)},
          date(year=2021, month=12, day=6): {'start': time(hour=3, minute=59),
                                             'duration': timedelta(hours=16, minutes=51)},
          date(year=2022, month=2, day=25): {'start': time(hour=6, minute=59),
                                             'duration': timedelta(hours=13, minutes=51)},
          date(year=2022, month=3, day=24): {'start': time(hour=6, minute=59),
                                             'duration': timedelta(hours=3, minutes=51)},
          date(year=2022, month=3, day=31): {'start': time(hour=6, minute=59),
                                             'duration': timedelta(hours=8, minutes=47)},  # 15:46 is closing time
          date(year=2022, month=9, day=12): {'start': time(hour=6, minute=59),
                                             'duration': timedelta(hours=13, minutes=51)},
          date(year=2024, month=8, day=14): {'start': time(hour=3, minute=59),
                                             'duration': timedelta(hours=16, minutes=51)},
          },
     InstrumentType.FUTURES:
     # {date.min: {'start': time(hour=7), 'duration': timedelta(hours=9, minutes=50)},
     #  date(year=2022, month=7, day=12): {'start': time(hour=7, minute=0),
     #                                     'duration': timedelta(hours=13, minutes=50)},
     #  date(year=2022, month=9, day=12): {'start': time(hour=5, minute=59),
     #                                     'duration': timedelta(hours=14, minutes=51)},
     #  date(year=2024, month=6, day=13): {'start': time(hour=6, minute=59),
     #                                     'duration': timedelta(hours=13, minutes=51)}
     #  }
         {date.min: {'start': time(hour=0), 'duration': timedelta(seconds=0)}}
     }
)
# break_in_working_hours is the list of tuples (duration of a break, when is the break)
break_in_working_hours = ExchangeIntervalTree(
    {InstrumentType.FUTURES: {date.min: [{'start': time(hour=11), 'duration': timedelta(minutes=5)}],
                              date(year=2022, month=9, day=12): [
                                  {'start': time(hour=11), 'duration': timedelta(minutes=5)},
                                  {'start': time(hour=15, minute=50), 'duration': timedelta(minutes=15)}]
                              },
     InstrumentType.STOCK: {start_date: [],
                            date(year=2018, month=3, day=13): [{'start': time(hour=7, minute=20),
                                                                'duration': timedelta(hours=0, minutes=10)},
                                                               {'start': time(hour=7, minute=40),
                                                                'duration': timedelta(hours=6, minutes=19)}],
                            date(year=2018, month=6, day=8): [{'start': time(hour=15, minute=40),
                                                               'duration': timedelta(minutes=5)}],
                            date(year=2020, month=6, day=22): [{'start': time(hour=15, minute=40),
                                                                'duration': timedelta(minutes=24)}],
                            date(year=2022, month=3, day=24): [],
                            date(year=2022, month=3, day=31): [{'start': time(hour=15, minute=40),
                                                                'duration': timedelta(minutes=5)}],
                            date(year=2022, month=9, day=12): [{'start': time(hour=15, minute=40),
                                                                'duration': timedelta(minutes=24)}]
                            }
     }
)

# duration of a session (also defined as open <= t < close)
session_type = ExchangeIntervalTree(
    {
        InstrumentType.FUTURES:
            {date(year=2022, month=9, day=12): [],
             date(year=2024, month=6, day=13): {'premarket': {'open': time(hour=5, minute=59),
                                                              'close': time(hour=6, minute=0)},
                                                'afterhours': {'open': time(hour=16, minute=5),
                                                               'close': time(hour=20, minute=50)}},
             date.max: {'premarket': {'open': time(hour=6, minute=59),
                                      'close': time(hour=7, minute=0)},
                        'afterhours': {'open': time(hour=16, minute=5),
                                       'close': time(hour=20, minute=50)}}
             },
        InstrumentType.STOCK:
            {start_date: {SessionPeriod.MAIN: {'start': time(hour=13, minute=59),
                                               'duration': timedelta(hours=10, minutes=1),
                                               'opening': True,
                                               'closing': False
                                               }},
             date(year=2018, month=3, day=13): {SessionPeriod.PREMARKET: {'start': time(hour=6, minute=59),
                                                                          'duration': timedelta(hours=0, minutes=41),
                                                                          'opening': True,
                                                                          'closing': False
                                                                          },
                                                SessionPeriod.MAIN: {'start': time(hour=13, minute=59),
                                                                     'duration': timedelta(hours=10, minutes=1),
                                                                     'opening': True,
                                                                     'closing': False
                                                                     }},
             date(year=2018, month=6, day=8): {SessionPeriod.MAIN: {'start': time(hour=6, minute=59),
                                                                    'duration': timedelta(hours=8, minutes=47),
                                                                    'opening': True,
                                                                    'closing': True
                                                                    }},  # 15:46 is closing time
             date(year=2020, month=6, day=22): {SessionPeriod.MAIN: {'start': time(hour=6, minute=59),
                                                                     'duration': timedelta(hours=8, minutes=41),
                                                                     'opening': True,
                                                                     'closing': False
                                                                     },
                                                SessionPeriod.AFTERHOURS: {'start': time(hour=16, minute=4),
                                                                           'duration': timedelta(hours=4, minutes=46),
                                                                           'opening': True,
                                                                           'closing': False
                                                                           }},
             date(year=2021, month=12, day=6): {SessionPeriod.PREMARKET: {'start': time(hour=3, minute=59),
                                                                          'duration': timedelta(hours=3, minutes=1),
                                                                          'opening': True,
                                                                          'closing': False
                                                                          },
                                                SessionPeriod.MAIN: {'start': time(hour=7, minute=0),
                                                                     'duration': timedelta(hours=8, minutes=40),
                                                                     'opening': True,
                                                                     'closing': False
                                                                     },
                                                SessionPeriod.AFTERHOURS: {'start': time(hour=16, minute=4),
                                                                           'duration': timedelta(hours=4, minutes=46),
                                                                           'opening': True,
                                                                           'closing': False
                                                                           }},
             date(year=2022, month=2, day=25): {SessionPeriod.MAIN: {'start': time(hour=6, minute=59),
                                                                     'duration': timedelta(hours=8, minutes=41),
                                                                     'opening': True,
                                                                     'closing': False
                                                                     },
                                                SessionPeriod.AFTERHOURS: {'start': time(hour=16, minute=4),
                                                                           'duration': timedelta(hours=4, minutes=46),
                                                                           'opening': True,
                                                                           'closing': False
                                                                           }},
             date(year=2022, month=3, day=24): {SessionPeriod.MAIN: {'start': time(hour=6, minute=59),
                                                                     'duration': timedelta(hours=3, minutes=41),
                                                                     'opening': True,
                                                                     'closing': False
                                                                     }},
             date(year=2022, month=3, day=31): {SessionPeriod.MAIN: {'start': time(hour=6, minute=59),
                                                                     'duration': timedelta(hours=8, minutes=47),
                                                                     'opening': True,
                                                                     'closing': True
                                                                     }},  # 15:46 is closing time
             date(year=2022, month=9, day=12): {SessionPeriod.MAIN: {'start': time(hour=6, minute=59),
                                                                     'duration': timedelta(hours=8, minutes=41),
                                                                     'opening': True,
                                                                     'closing': False
                                                                     },
                                                SessionPeriod.AFTERHOURS: {'start': time(hour=16, minute=4),
                                                                           'duration': timedelta(hours=4, minutes=46),
                                                                           'opening': True,
                                                                           'closing': False
                                                                           }},
             date(year=2024, month=8, day=14): {SessionPeriod.PREMARKET: {'start': time(hour=3, minute=59),
                                                                          'duration': timedelta(hours=3, minutes=1),
                                                                          'opening': True,
                                                                          'closing': False
                                                                          },
                                                SessionPeriod.MAIN: {'start': time(hour=7, minute=0),
                                                                     'duration': timedelta(hours=8, minutes=40),
                                                                     'opening': True,
                                                                     'closing': False
                                                                     },
                                                SessionPeriod.AFTERHOURS: {'start': time(hour=16, minute=4),
                                                                           'duration': timedelta(hours=4, minutes=46),
                                                                           'opening': True,
                                                                           'closing': False
                                                                           }}
             }
    }
)

holidays = ([date(year=year, month=1, day=day) for day in range(1, 3) for year in range(2017, 2030)] +
            [date(year=year, month=1, day=7) for year in range(2017, 2030)] +
            [date(year=year, month=2, day=23) for year in range(2017, 2030)] +
            [date(year=year, month=3, day=8) for year in range(2017, 2030)] +
            [date(year=year, month=5, day=1) for year in range(2017, 2030)] +
            [date(year=year, month=5, day=9) for year in range(2017, 2030)] +
            [date(year=year, month=6, day=12) for year in range(2017, 2030)] +
            [date(year=year, month=11, day=4) for year in range(2017, 2030)] +
            [date(year=2022, month=2, day=day) for day in range(26, 29)] +
            [date(year=2022, month=3, day=day) for day in range(1, 24)])

working_weekends = (
        [date(year=2024, month=4, day=27), date(year=2024, month=11, day=2), date(year=2024, month=12, day=28)] +
        [date(year=2022, month=3, day=5)])
