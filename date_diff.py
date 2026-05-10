import datetime
target = datetime.date(2026, 5, 10)

fws_max = datetime.date(2016, 5, 22)
print("fws_diff:", (target - fws_max).days)

kpi_max = datetime.date(2024, 1, 1)
print("kpi_diff:", (target - kpi_max).days)

ccc_max = datetime.date(2020, 7, 20)
print("ccc_diff:", (target - ccc_max).days)

pdf_max = datetime.date(2023, 12, 31)
print("pdf_diff:", (target - pdf_max).days)
