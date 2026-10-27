"""Large-scale integration tests for glob-aware event handler dispatch."""

from collections import Counter
from collections.abc import Callable

import pytest

from apixis.core.event.base import ApixEvent, ApixEventHandler, EventType
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.factory import get_event_registry
from apixis.core.event.factory import get_handler_registry


# These workloads are intentionally large enough to exercise real cache and
# wildcard matching paths while remaining suitable for a personal computer.
MATRIX_EVENT_COUNT = 20_000
OBSERVED_EVENT_COUNT = 30_000
DISPATCH_EVENT_COUNT = 2_400


async def _noop_handler(event: ApixEvent) -> None:
    """Provide a minimal callback for chain-resolution-only handlers."""


def _make_handler(
    name: str,
    subscribe: list[str],
    *,
    priority: float,
    filter_event: list[str] | None = None,
    callback: Callable | None = None,
) -> ApixEventHandler:
    """Build one handler entry for an integration-test registry."""
    entry = ApixEventHandler(_noop_handler if callback is None else callback)
    entry.name = name
    entry._register_order = 0
    entry.subscribe = subscribe
    entry.filter_event = [] if filter_event is None else filter_event
    entry.priority = priority
    return entry


def _observe_event(event_name: str, event_index: int) -> None:
    """Record one exact event name without retaining its event object."""
    get_event_registry().record_event(
        ApixEvent(
            event_id=f"scale-event-{event_index}",
            event_type=EventType.WORKFLOW,
            event_name=event_name,
            context=None,
            timestamp=0,
        )
    )


def test_twenty_thousand_event_glob_matrix_resolves_exact_ordered_chains():
    """Resolve a large matrix containing every supported wildcard form."""
    handlers = [
        _make_handler("global", ["tenant.*"], priority=100),
        _make_handler(
            "build",
            ["tenant.0??.service.??.build.*"],
            priority=80,
        ),
        _make_handler(
            "successful_deploy",
            ["tenant.0[0-4]?.service.??.deploy.*"],
            filter_event=["*.failed"],
            priority=70,
        ),
        _make_handler(
            "successful_test",
            ["tenant.???.service.??.test.passed"],
            priority=60,
        ),
        _make_handler(
            "low_service_release",
            ["tenant.???.service.0?.release.*"],
            priority=50,
        ),
        _make_handler("wrong_case", ["Tenant.*"], priority=200),
    ]
    handlers.extend(
        _make_handler(
            f"tenant_shard_{digit}",
            [f"tenant.??{digit}.service.*"],
            priority=10,
        )
        for digit in range(10)
    )
    for handler in handlers:
        get_handler_registry().register_handler(handler)

    actions = ("build", "deploy", "test", "release", "audit")
    statuses = ("passed", "failed")
    match_counts: Counter[str] = Counter()
    event_index = 0

    for tenant in range(100):
        for service in range(20):
            for action in actions:
                for status in statuses:
                    event_name = (
                        f"tenant.{tenant:03d}.service.{service:02d}."
                        f"{action}.{status}"
                    )
                    _observe_event(event_name, event_index)
                    event_index += 1

                    expected_chain = ["global"]
                    if action == "build":
                        expected_chain.append("build")
                    if action == "deploy" and tenant < 50 and status == "passed":
                        expected_chain.append("successful_deploy")
                    if action == "test" and status == "passed":
                        expected_chain.append("successful_test")
                    if action == "release" and service < 10:
                        expected_chain.append("low_service_release")
                    expected_chain.append(f"tenant_shard_{tenant % 10}")

                    chain = (
                        get_handler_registry().get_handlers_chain_for_event(
                            event_name
                        )
                    )
                    assert chain == expected_chain, event_name
                    match_counts.update(chain)

    assert event_index == MATRIX_EVENT_COUNT
    # Resolution stays correct after the workload exceeds the bounded cache.
    assert len(get_handler_registry().cached_chain) == 8192
    assert "tenant.000.service.00.build.passed" not in get_handler_registry().cached_chain
    assert len(get_event_registry().get_registered_events()) == MATRIX_EVENT_COUNT
    assert match_counts == Counter(
        {
            "global": 20_000,
            "build": 4_000,
            "successful_deploy": 1_000,
            "successful_test": 2_000,
            "low_service_release": 2_000,
            **{f"tenant_shard_{digit}": 2_000 for digit in range(10)},
        }
    )
    assert get_handler_registry().get_unmatched_subscriptions("wrong_case") == [
        "Tenant.*"
    ]


def test_thirty_thousand_observed_events_keep_handler_chains_lazy():
    """Build only explicitly queried chains after wildcard registration."""
    get_handler_registry().register_handler(
        _make_handler("stream_global", ["stream.*"], priority=100)
    )
    for region in "abcdef":
        get_handler_registry().register_handler(
            _make_handler(
                f"region_{region}",
                [f"stream.{region}.*"],
                priority=90,
            )
        )
    for digit in range(10):
        get_handler_registry().register_handler(
            _make_handler(
                f"stream_shard_{digit}",
                [f"stream.*.????{digit}.orders.*"],
                priority=10,
            )
        )
    get_handler_registry().register_handler(
        _make_handler("uppercase_only", ["Stream.*"], priority=200)
    )

    event_index = 0
    for region in "abcdef":
        for tenant in range(5_000):
            level = "info" if tenant % 2 == 0 else "debug"
            _observe_event(
                f"stream.{region}.{tenant:05d}.orders.{level}",
                event_index,
            )
            event_index += 1
    for index in range(100):
        _observe_event(
            f"Stream.a.{index:05d}.orders.info",
            event_index,
        )
        event_index += 1

    get_handler_registry().register_handler(
        _make_handler(
            "regional_orders_info",
            ["stream.[a-c].?????.orders.*"],
            filter_event=["*.debug"],
            priority=50,
        )
    )

    assert event_index == OBSERVED_EVENT_COUNT + 100
    assert get_handler_registry().cached_chain == {}

    for region in "abc":
        for tenant in range(0, 5_000, 2):
            event_name = f"stream.{region}.{tenant:05d}.orders.info"
            assert get_handler_registry().get_handlers_chain_for_event(
                event_name
            ) == [
                "stream_global",
                f"region_{region}",
                "regional_orders_info",
                f"stream_shard_{tenant % 10}",
            ]

    assert len(get_handler_registry().cached_chain) == 7_500
    assert "stream.a.00001.orders.debug" not in (
        get_handler_registry().cached_chain
    )
    assert "stream.d.00000.orders.info" not in (
        get_handler_registry().cached_chain
    )
    assert "Stream.a.00000.orders.info" not in (
        get_handler_registry().cached_chain
    )
    assert get_handler_registry().get_unmatched_subscriptions(
        "uppercase_only"
    ) == []


@pytest.mark.asyncio
async def test_two_thousand_four_hundred_events_dispatch_through_glob_handlers(wait_for_dispatch):
    """Publish and execute thousands of events through the complete runtime."""
    call_counts: Counter[str] = Counter()

    def counting_callback(name: str):
        async def callback(event: ApixEvent) -> None:
            call_counts[name] += 1
            event.context.append(name)

        return callback

    handlers = [
        _make_handler(
            "global",
            ["api.v?.tenant.????.*"],
            priority=100,
            callback=counting_callback("global"),
        ),
        _make_handler(
            "v1_mutation",
            ["api.v1.tenant.????.[ob]*.create.success"],
            priority=75,
            callback=counting_callback("v1_mutation"),
        ),
        _make_handler(
            "successful_orders",
            ["api.v[12].tenant.????.orders.*"],
            filter_event=["*.error"],
            priority=50,
            callback=counting_callback("successful_orders"),
        ),
        _make_handler(
            "wrong_case",
            ["API.*"],
            priority=200,
            callback=counting_callback("wrong_case"),
        ),
    ]
    for handler in handlers:
        get_handler_registry().register_handler(handler)

    pipe = ApixEventPipe()
    event_loop = ApixEventLoop(get_handler_registry(), pipe, get_event_registry())
    variants = (
        ("api.v1.tenant.{tenant}.orders.create.success", [
            "global",
            "v1_mutation",
            "successful_orders",
        ]),
        ("api.v1.tenant.{tenant}.orders.create.error", ["global"]),
        ("api.v2.tenant.{tenant}.billing.read.success", ["global"]),
        ("api.v2.tenant.{tenant}.users.read.success", ["global"]),
    )
    dispatched = 0

    await pipe.start()
    await event_loop.start()
    try:
        for tenant in range(600):
            tenant_id = f"{tenant:04d}"
            for event_pattern, expected_trace in variants:
                event_name = event_pattern.format(tenant=tenant_id)
                event = ApixEvent(event_name, EventType.WORKFLOW, event_name, [], 0)
                await pipe.put(event)
                await wait_for_dispatch(event_loop)
                assert event.accepted is False
                assert event.context == expected_trace
                dispatched += 1
    finally:
        await event_loop.stop()
        await pipe.stop()

    assert dispatched == DISPATCH_EVENT_COUNT
    assert call_counts == Counter(
        {
            "global": 2_400,
            "v1_mutation": 600,
            "successful_orders": 600,
        }
    )
    assert len(get_handler_registry().cached_chain) == DISPATCH_EVENT_COUNT
    assert len(get_event_registry().get_registered_events()) == (
        DISPATCH_EVENT_COUNT
    )
