import datetime
import calendar

def date_by_week_n_from(year, week):
    d_from = f"{year}-W{week}"
    return datetime.datetime.strptime(d_from + '-1', "%Y-W%W-%w").strftime("%Y.%d.%m")


def date_by_week_n_to(year, week):
    d_to = f"{year}-W{week + 1}"
    return datetime.datetime.strptime(d_to + '-0', "%Y-W%W-%w").strftime("%Y.%d.%m")


def date_by_month_from(year, month):
    return f"{year}.1.{month}"


def date_by_month_to(year, month):
    day = calendar.monthrange(year, month)[1]
    return f"{year}.{day}.{month}"


def split_by_category(lst, category):
    last_category = lst[0][category]
    result = [[]]
    for i in lst:
        if last_category == i[category]:
            result[-1].append(i)
        else:
            last_category = i[category]
            result.append([i])
    return result
