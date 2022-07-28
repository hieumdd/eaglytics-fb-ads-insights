import pytest

from facebook.pipeline import pipelines
from facebook.facebook_service import pipeline_service

TIMEFRAME = [
    ("auto", (None, None)),
    ("manual", ("2022-01-01", "2022-07-27")),
]


@pytest.fixture(params=[i[1] for i in TIMEFRAME], ids=[i[0] for i in TIMEFRAME])
def timeframe(request):
    return request.param


@pytest.fixture(
    params=pipelines.values(),
    ids=pipelines.keys(),
)
def pipeline(request):
    return request.param


def test_service(pipeline, timeframe):
    res = pipeline_service(pipeline)(*timeframe)
    res
