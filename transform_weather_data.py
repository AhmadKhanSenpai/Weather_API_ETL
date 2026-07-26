from datetime import datetime as dt
from datetime import timedelta as td


def get_date_range():
    """
    this will provide the date range i need for request 'start_date' and 'end_date parameters
    it returns a tuple which contains start and end date
    """

    end_date = dt.now().strftime("%Y-%m-%d")
    diff = dt.now() - td(days=7)
    start_date = diff.strftime("%Y-%m-%d")

    return (start_date, end_date)


# testing
if __name__ == "__main__":
    date = get_date_range()
    print(f"end_date = {date[1]}")
    print(f"start_date = {date[0]}")
