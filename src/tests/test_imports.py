from __future__ import annotations


def test_pydantic_import_guard():
    import djoutbox.publisher as pub
    from pydantic import BaseModel
    assert pub.BaseModel is BaseModel


def test_publisher_exports_lazy():
    import djoutbox
    msg = djoutbox.OutboxMessage("k", "v")
    assert msg.routing_key == "k"
