from __future__ import annotations

import pickle

from app.core.errors import APIError, NotFoundError, ServiceUnavailableError


def test_api_error_pickles_with_full_payload():
    exc = APIError(
        503,
        "service_unavailable",
        "loader failed",
        {"loader_type": "WebLoader"},
        "Error",
    )

    restored = pickle.loads(pickle.dumps(exc))

    assert isinstance(restored, APIError)
    assert restored.status_code == 503
    assert restored.code == "service_unavailable"
    assert restored.message == "loader failed"
    assert restored.details == {"loader_type": "WebLoader"}
    assert restored.rag_lib_exception_type == "Error"
    assert str(restored) == "loader failed"


def test_specialized_api_errors_remain_picklable():
    not_found = pickle.loads(pickle.dumps(NotFoundError("missing artifact", {"artifact_id": "a1"})))
    assert isinstance(not_found, NotFoundError)
    assert not_found.message == "missing artifact"
    assert not_found.details == {"artifact_id": "a1"}
    assert str(not_found) == "missing artifact"

    service_unavailable = pickle.loads(
        pickle.dumps(
            ServiceUnavailableError(
                "playwright browser missing",
                {"loader_type": "WebLoader"},
                "Error",
            )
        )
    )
    assert isinstance(service_unavailable, ServiceUnavailableError)
    assert service_unavailable.message == "playwright browser missing"
    assert service_unavailable.details == {"loader_type": "WebLoader"}
    assert service_unavailable.rag_lib_exception_type == "Error"
    assert str(service_unavailable) == "playwright browser missing"
