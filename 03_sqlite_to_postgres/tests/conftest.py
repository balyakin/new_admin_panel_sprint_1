"""Configuration for pytest"""


def pytest_addoption(parser):
    parser.addoption("--db_name", action="store", default="movies_database")
    parser.addoption("--user", action="store", default="app")
    parser.addoption("--password", action="store", default="123qwe")
    parser.addoption("--host", action="store", default="127.0.0.1")
    parser.addoption("--port", action="store", default="5432")


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.db_name
    if 'db_name' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("db_name", [option_value])
    option_value = metafunc.config.option.user
    if 'user' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("user", [option_value])
    option_value = metafunc.config.option.password
    if 'password' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("password", [option_value])
    option_value = metafunc.config.option.host
    if 'host' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("host", [option_value])
    option_value = metafunc.config.option.port
    if 'port' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("port", [option_value])
