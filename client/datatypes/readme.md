## Datatypes 

```python
    class Candle
```
This dataclass stores the metadata (not the .csv file itself!!). The metadata includes the
following attributes:

* `lastCandle: datetime` - provides the tz-aware datetime of a last candle 
* `startOfDayIdx: list` - contains indices of a candles.csv file, pointing at the start of the day
* `days: list` - contains `datetime.date` objects which equal real days of candles listed in the candles.csv file
* `prevDaysPrice: list` - contains the last price captured at the last candle (close value) of the previous day in correspondence with the `startOfDayIdx` or `days` attributes.

Currently the downside of this class is that we need to traverse rows of the candles.csv `pd.DataFrame` in order to initialize an object, which is extremely suboptimal. Probably `numba` improvement might help.

```python
class Ticker
```

This dataclass handles the computation, storing and retrieval of any statistics related to a given instrument.
Methods which realize this idea:
* `rmQuery(rms: list)` takes list `rms` containing the names of realized measures as in the keys of `realizedMeasures` dict and computes/updates/loads all of these realized measures. Returns a `pd.DataFrame` with each column corresponding to a requested realized measure.