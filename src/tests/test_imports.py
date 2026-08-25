from __future__ import annotations


def test_publisher_exports_lazy():
    import djoutbox

    msg = djoutbox.OutboxMessage("k", "v")
    assert msg.routing_key == "k"
