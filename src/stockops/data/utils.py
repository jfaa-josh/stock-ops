def get_db_path(db_date: date | None = None) -> Path:
    """
    Returns the path to the .db file for the given date.
    If no date is given, uses today's date.
    """
    if db_date is None:
        db_date = date.today()
    db_name = db_date.strftime("%Y-%m-%d") + ".db"
    return RAW_REALTIME_DIR / db_name
