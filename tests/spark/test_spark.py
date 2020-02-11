import pytest  # noqa F401
from birgitta import context
from birgitta import spark as bspark


def test_conf():
    # Prevent bspark.is_local() returning true
    orig_ctx_session_type = context.get("BIRGITTA_SPARK_SESSION_TYPE")
    context.set("BIRGITTA_SPARK_SESSION_TYPE", "NONLOCAL")
    session = bspark.session()
    assert session.conf.get("spark.sql.session.timeZone") == "UTC"
    context.set("BIRGITTA_SPARK_SESSION_TYPE", orig_ctx_session_type)
