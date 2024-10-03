from datetime import datetime, date, time

from numba import njit
import numpy as np

candlePath = f"/Users/s/Desktop/segr/candles/"
instrumentPath = f"/Users/s/Desktop/segr/instruments/"

tickers = ['KZIZP', 'IRKT', 'VSMO', 'UNAC', 'VKCO', 'TTLK', 'MGNT', 'SPBE', 'SVCB', 'KZIZ', 'ETLN', 'KZOSP', 'WUSH', 'GEMC', 'UGLD', 'PHOR', 'HNFG', 'HHRU', 'LNZL', 'SELG', 'TATNP', 'SLAV', 'PRFN', 'MAGN', 'VTBR', 'CARM', 'RUAL', 'NKHP', 'TGKJ', 'BANEP', 'OKEY', 'ALRS', 'ELMT', 'MRKP', 'FLOT', 'DIAS', 'TATN', 'ABIO', 'UWGN', 'DVEC', 'RTKM', 'ZAYM', 'MTSS', 'TGKN', 'TRNFP', 'FEES', 'OBNE', 'PMSB', 'ZILLP', 'MSRS', 'IRAO', 'NSVZ', 'GCHE', 'SNGSP', 'NVTK', 'UNKL', 'NKNC', 'AQUA', 'VRSB', 'MBNK', 'MOEX', 'ROLO', 'OZON', 'OGKB', 'NOMP', 'GLTR', 'KAZTP', 'SNGS', 'CBOM', 'AMEZ', 'TGKBP', 'ABRD', 'PIKK', 'ROSN', 'EUTR', 'TRMK', 'SOFL', 'MRKU', 'KRKNP', 'CHMF', 'ENPG', 'POLY', 'NKNCP', 'OBNEP', 'UFOSP', 'MRKV', 'LSRG', 'TCSG', 'CNTL', 'SVAV', 'RTKMP', 'KMAZ', 'KZOS', 'FIXP', 'MGTSP', 'ELFV', 'KLVZ', 'AGRO', 'VEON-RX', 'NTZL', 'BLNG', 'DSKY', 'GRNT', 'SMLT', 'BANE', 'AFLT', 'CIAN', 'ORUP', 'SBER', 'RBCM', 'GECO', 'GTRK', 'MVID', 'PMSBP', 'MSTT', 'MTLR', 'IVAT', 'AKRN', 'MDMG', 'GAZP', 'SBERP', 'LENT', 'BSPB', 'ASTR', 'RKKE', 'LSNG', 'RENI', 'MRKC', 'POSI', 'KROT', 'KAZT', 'SIBN', 'YAKG', 'TGKB', 'KLSB', 'CHMK', 'RNFT', 'MRKZ', 'APTK', 'QIWI', 'HYDR', 'NLMK', 'BELU', 'LNZLP', 'YNDX', 'SFIN', 'FESH', 'RASP', 'NOMPP', 'SGZH', 'LSNGP', 'GMKN', 'LIFE', 'UPRO', 'MRKS', 'MSNG', 'AFKS', 'PLZL', 'LKOH', 'DELI', 'MGKL', 'LEAS', 'UDMN', 'NMTP', 'TGKA', 'CNTLP', 'MTLRP', 'MRKY', 'LEZ4', 'HSZ4', 'BEZ4', 'WUU4', 'CRU4', 'TNZ4', 'HSU4', 'HKU4', 'PIU4', 'NKU4', 'S0U4', 'SVM5', 'NGN4', 'ISU4', 'FEZ4', 'PIZ4', 'R2M5', 'W4X4', 'MNZ4', 'IMOEXF', 'FNU4', 'W4F5', 'MMZ4', 'MMH6', 'BRG5', 'EDH5', 'SGZ4', 'CoN4', 'CoU4', 'EuZ5', 'SXH5', 'BDU4', 'CRM5', 'RUU4', 'WUM5', 'MNH5', 'CMH5', 'PDU4', 'LEU4', 'NAM5', 'MMM7', 'W4V4', 'NGV4', 'USDRUBF', 'MMU5', 'MVU4', 'NAZ4', 'BEU4', 'NlQ4', 'RNU4', 'MCU4', 'KZU4', 'PHZ4', 'VIQ4', 'DXH5', 'RBU4', 'CAU4', 'CMM5', 'GDM5', 'KZH5', 'LKZ4', 'TYH5', 'AUU4', 'SNZ4', 'R2H5', 'W4U4', 'VKM5', 'CHU4', 'I2Z4', 'HKZ4', 'TTZ4', 'MTU4', 'BSM5', 'PZU4', 'GKU4', 'EDZ4', 'R2Z4', 'HYZ4', 'I2M5', 'EuZ4', 'ASZ4', 'GLDRUBF', 'RNZ5', 'AKZ4', 'RIU4', 'GUU4', 'BRN4', 'BNZ4', 'HOZ4', 'RNH6', 'RIH5', 'ALZ4', 'SNU4', 'MTZ4', 'SFZ4', 'BNU4', 'TPU4', 'MVZ4', 'SVZ4', 'SRZ4', 'SRM5', 'TTH5', 'CMU4', 'UCZ4', 'BNH5', 'R2U4', 'CRH5', 'SXZ4', 'BSZ4', 'SPU4', 'SVH5', 'MGU4', 'IRU4', 'MMZ7', 'CRZ5', 'DJZ4', 'RMU4', 'PTH5', 'NGX4', 'DJH5', 'CRU5', 'JPZ4', 'WUH5', 'VKH5', 'SPZ4', 'W4K5', 'RTZ4', 'N2Z4', 'MMM5', 'SiU5', 'MGZ4', 'SuV4', 'W4J5', 'NMU4', 'ALU4', 'SuZ4', 'GLU4', 'HOU4', 'RIZ5', 'SZH5', 'BSU4', 'CAZ4', 'TPZ4', 'VKU4', 'BEM5', 'BRQ4', 'SZU4', 'ARH5', 'RTU4', 'I2U4', 'LKU4', 'ISZ4', 'EuM5', 'PSU4', 'MMZ6', 'WUZ4', 'N2M5', 'SAV4', 'N2U4', 'BDZ4', 'NKZ4', 'MMH7', 'VKZ4', 'MXM5', 'TYM5', 'CMZ4', 'JPU4', 'SRU4', 'SRH5', 'AFH5', 'RLU4', 'RUZ4', 'RIM6', 'AEM5', 'SiU4', 'ARZ4', 'DXM5', 'MEZ4', 'IRZ4', 'POU4', 'MTH5', 'PSZ4', 'MMU4', 'AKU4', 'BSH5', 'CRZ4', 'PDH5', 'RIZ4', 'TYU4', 'UCU4', 'RMH5', 'RIM5', 'GDH5', 'KMU4', 'MAZ4', 'UCH5', 'CoQ4', 'SZZ4', 'RNH5', 'RNU5', 'SFU4', 'RLZ4', 'NlN4', 'UCM5', 'GZM6', 'NMH5', 'GDZ4', 'MVM5', 'TYZ4', 'FSU4', 'CSU4', 'GDU4', 'SiH6', 'HSH5', 'NlU4', 'RNM5', 'SXU4', 'ALH5', 'GZH5', 'SuN4', 'MMM6', 'GLH5', 'TTU4', 'AUZ4', 'HKH5', 'BRX4', 'EURRUBF', 'RNM6', 'MCZ4', 'NGZ4', 'FLH5', 'MMH5', 'SGU4', 'RMZ4', 'GZZ5', 'AFZ4', 'SiM5', 'TIU4', 'GZM5', 'SOU4', 'SuU4', 'RIU5', 'PHU4', 'SSU4', 'S0Z4', 'W4N4', 'N2H5', 'RNZ4', 'PDZ4', 'SiM6', 'AEU4', 'KMM5', 'SFM5', 'RIH6', 'OGU4', 'SCU4', 'KMZ4', 'MXU4', 'SOZ4', 'BEH5', 'W4Q4', 'MVH5', 'RBZ4', 'MEU4', 'MXH5', 'CSZ4', 'SiH5', 'SZM5', 'NGU4', 'SSZ4', 'TNU4', 'HSM5', 'GKZ4', 'AEZ4', 'RMM5', 'PTU4', 'ARM5', 'W4Z4', 'BRU4', 'FLZ4', 'FNZ4', 'CFU4', 'MXZ4', 'PZZ4', 'KZM5', 'SCZ4', 'GUZ4', 'PTZ4', 'CNYRUBF', 'LKM5', 'EuU4', 'SiZ5', 'SAN4', 'BRJ5', 'FEU4', 'HYU4', 'HOH5', 'HKM5', 'GLZ4', 'SVU4', 'GZU5', 'DXZ4', 'KMH5', 'DXU4', 'MMU6', 'KZZ4', 'EuU5', 'AEH5', 'ARU4', 'CHZ4', 'W4M5', 'SuX4', 'EuH5', 'BRF5', 'LKH5', 'W4H5', 'GLM5', 'AFU4', 'BRM5', 'CFZ4', 'BRV4', 'SFH5', 'GZH6', 'FLM5', 'NMZ4', 'SuQ4', 'NAU4', 'W4G5', 'MMZ5', 'ASU4', 'GZZ4', 'DJM5', 'NAH5', 'BRZ4', 'NGQ4', 'BBU4', 'SiZ4', 'FLU4', 'I2H5', 'OGZ4', 'GZU4', 'POZ4', 'MAU4', 'VIN4', 'SXM5', 'MNU4', 'FSZ4', 'BNM5', 'DJU4', 'BRK5', 'EDU4']

# time of the start of the candle
workingHours = {date(year=2018, month=6, day=7): {'open': time(hour=14, minute=0), 'close': time(hour=23, minute=59)},
                date.max: {'open': time(hour=7, minute=0), 'close': time(hour=15, minute=59)}}
workingHoursNumba = {np.datetime64('2018-06-07'): {'open': np.datetime64(hour=14, minute=0), 'close': time(hour=23, minute=59)},
                np.datetime64(date.max): {'open': time(hour=7, minute=0), 'close': time(hour=15, minute=59)}}

holidays = ([date(year=year, month=1, day=day) for day in range(1, 3) for year in range(2017, 2030)] +
            [date(year=year, month=1, day=7) for year in range(2017, 2030)] +
            [date(year=year, month=2, day=23) for year in range(2017, 2030)] +
            [date(year=year, month=3, day=8) for year in range(2017, 2030)] +
            [date(year=year, month=5, day=1) for year in range(2017, 2030)] +
            [date(year=year, month=5, day=9) for year in range(2017, 2030)] +
            [date(year=year, month=6, day=12) for year in range(2017, 2030)] +
            [date(year=year, month=11, day=4) for year in range(2017, 2030)])

workingWeekends = ([date(year=2024, month=4, day=27), date(year=2024, month=11, day=2), date(year=2024, month=12, day=28)] +
                   [date(year=2022, month=3, day=5)])


def getWorkingHours(day: date) -> dict[str, time]:
    breakDays = sorted(workingHours.keys())

    for breakDay in breakDays:
        if day <= breakDay:
            return workingHours[breakDay]

@njit
def getWorkingHoursNumba(day: date) -> dict[str, np.datetime64]:
    breakDays = sorted(workingHours.keys())

    for breakDay in breakDays:
        if day <= breakDay:
            return workingHours[breakDay]