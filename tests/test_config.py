from traxit_manage.config import configure_database


def test_configure_database():
    db = configure_database('dbname')
    assert db is not None
