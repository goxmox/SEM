import api.tinvest.constants as t_constants
from engine.schemas.datatypes import Broker
from decimal import Decimal

t_invest = Broker(
    broker_name='t_invest',
    break_in_working_hours=t_constants.break_in_working_hours,
    holidays=t_constants.holidays,
    working_hours=t_constants.working_hours,
    start_date=t_constants.start_date,
    working_weekends=t_constants.working_weekends,
    session_type=t_constants.session_type,
    commission=Decimal('0.0004')
)