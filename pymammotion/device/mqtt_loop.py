"""MQTT-side cadence driver for ``DeviceHandle``.

Pulled out of ``handle.py`` so the loop body — and the per-mode poll-interval
table that drives it — can be read and tuned in isolation from the rest of the
facade.

The loop is a free coroutine that takes the owning ``DeviceHandle`` rather than
a method on it.  All state (transports, rearm event, last-send timestamps,
``_stopping``, …) is read directly off the handle; the loop owns nothing.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING

from pymammotion.device.ble_loop import _BLE_MODE_RECHECK_INTERVAL
from pymammotion.device.modes import _DeviceMode
from pymammotion.transport.base import Transport, TransportType

if TYPE_CHECKING:
    from pymammotion.device.handle import DeviceHandle

_logger = logging.getLogger(__name__)

#: Activity-loop backoff when MQTT is rate-limited and no BLE is available.
_RATE_LIMITED_BACKOFF: float = 43200.0  # 12 hours

#: MQTT one-shot (count=1) poll cadence per device mode.  Tuned for cloud quotas.
#: Each entry can be overridden at process startup via an environment variable:
#: MAMMOTION_POLL_ACTIVE_SECS, MAMMOTION_POLL_DOCKED_CHARGING_SECS,
#: MAMMOTION_POLL_DOCKED_FULL_SECS, MAMMOTION_POLL_IDLE_SECS
_MQTT_POLL_INTERVAL: dict[_DeviceMode, float] = {
    _DeviceMode.ACTIVE: float(os.environ.get("MAMMOTION_POLL_ACTIVE_SECS", 5 * 60)),
    _DeviceMode.DOCKED_CHARGING: float(os.environ.get("MAMMOTION_POLL_DOCKED_CHARGING_SECS", 30 * 60)),
    _DeviceMode.DOCKED_FULL: float(os.environ.get("MAMMOTION_POLL_DOCKED_FULL_SECS", 60 * 60)),
    _DeviceMode.IDLE: float(os.environ.get("MAMMOTION_POLL_IDLE_SECS", 15 * 60)),
}

_MQTT_NEW_POLL_INTERVAL: dict[_DeviceMode, float] = {
    _DeviceMode.ACTIVE: float(os.environ.get("MAMMOTION_POLL_ACTIVE_SECS", 5 * 60)),
    _DeviceMode.DOCKED_CHARGING: float(os.environ.get("MAMMOTION_POLL_DOCKED_CHARGING_SECS", 5 * 60)),
    _DeviceMode.DOCKED_FULL: float(os.environ.get("MAMMOTION_POLL_DOCKED_FULL_SECS", 60 * 60)),
    _DeviceMode.IDLE: float(os.environ.get("MAMMOTION_POLL_IDLE_SECS", 10 * 60)),
}

#: ACTIVE-mode (mowing) cadence overlays applied on top of the base table above,
#: densest-wins.  Only bite on the MQTT/cloud path — a live BLE stream feeds ~1/s
#: and this loop defers to it entirely (``ble_stream_active``).
#: 1) For a window after (re)entering ACTIVE — poll densely while a mow is fresh
#:    (or we just fell back to cloud), when the user is most likely watching/adjusting.
_ACTIVE_RESUME_DENSE_WINDOW: float = 10 * 60
_ACTIVE_RESUME_DENSE_INTERVAL: float = 2 * 60
#: 2) While the battery is low — track it closely on its way back to the dock.  Set
#:    generously (second half of a run) so the denser cadence starts well before the
#:    finish/return, not only once nearly drained (22% left the dense window too late,
#:    2026-07-29).
_ACTIVE_LOW_BATTERY_THRESHOLD: int = 30
_ACTIVE_LOW_BATTERY_INTERVAL: float = 3 * 60
#: 3) While a program is near completion (progress strictly above this %) — poll
#:    faster so the finish + return-to-dock transition is caught promptly instead of
#:    up to a full base interval late.
_ACTIVE_NEAR_END_PROGRESS_THRESHOLD: int = 96
_ACTIVE_NEAR_END_INTERVAL: float = 3 * 60
#: While RETURNING to the dock — poll at least this often (heading home; each cloud
#: poll also nudges a BLE reconnect via send_raw(prefer_ble) once BLE is usable).
_ACTIVE_RETURNING_INTERVAL: float = 2 * 60
#: 4) While within this many minutes before the device's non-work window starts —
#:    poll faster so Luba's forced return-to-dock at the deadline is caught promptly.
_ACTIVE_PRE_NONWORK_WINDOW_MIN: int = 20
_ACTIVE_PRE_NONWORK_INTERVAL: float = 3 * 60

#: DOCKED-mode overlay: while docked (charging) but NOT yet on BLE, poll densely so a
#: cloud RPT goes through ``send_raw(prefer_ble)`` — which schedules a BLE reconnect
#: when BLE is disconnected-but-usable — hopping us back onto (free) BLE fast instead
#: of sitting on cloud for a full DOCKED interval.  Gated on ``ble.is_usable`` so cloud
#: sends are spent ONLY when a reconnect can actually succeed (cached advert in range,
#: not in post-fail cooldown, RSSI ok); otherwise it would hammer the cloud budget
#: against an unreachable BLE.  Additionally bounded to the dock-settling window
#: (``handle._recently_docked()`` — first ``_RECENT_DOCK_WINDOW`` after docking) so a
#: persistent usable-but-not-connecting BLE can't drive dense polling forever.
#: Self-terminates: on reconnect ``is_connected`` flips True → the gate drops → back to
#: the normal DOCKED cadence.
_DOCKED_BLE_RECONNECT_INTERVAL: float = 20.0


def poll_interval(handle: DeviceHandle) -> tuple[float, str]:
    """Return the MQTT one-shot poll interval + the reason that selected it.

    Base cadence comes from ``_MQTT_POLL_INTERVAL`` (or ``_MQTT_NEW_POLL_INTERVAL``
    on newer firmware).  In ACTIVE (mowing) the base is tightened by three overlays,
    densest-wins:

    * for ``_ACTIVE_RESUME_DENSE_WINDOW`` s after (re)entering ACTIVE — every
      ``_ACTIVE_RESUME_DENSE_INTERVAL`` s;
    * while battery < ``_ACTIVE_LOW_BATTERY_THRESHOLD`` % — every
      ``_ACTIVE_LOW_BATTERY_INTERVAL`` s;
    * while progress > ``_ACTIVE_NEAR_END_PROGRESS_THRESHOLD`` % (program nearly
      done) — every ``_ACTIVE_NEAR_END_INTERVAL`` s;
    * within ``_ACTIVE_PRE_NONWORK_WINDOW_MIN`` min before the non-work window
      starts — every ``_ACTIVE_PRE_NONWORK_INTERVAL`` s;
    * while RETURNING to the dock — every ``_ACTIVE_RETURNING_INTERVAL`` s (heading
      home; each cloud poll also nudges a BLE reconnect once BLE is usable).

    The ``reason`` string (``base:<mode>``, ``resume-window``, ``returning``,
    ``low-battery``, ``near-end``, ``pre-nonwork``) names the winning overlay so the poll-cadence
    sensor and the loop's debug log can explain the chosen interval instead of
    leaving a bare number to reverse-engineer (see the 2026-07-26 "why 180s?"
    investigation).  A DEBUG line logs the raw inputs (battery/progress/minutes
    to non-work) behind each ACTIVE decision.

    Overlays bite only on the MQTT/cloud path — a live BLE stream feeds ~1/s and
    this loop defers to it (``ble_stream_active``), so the resume window is timed
    from the first cloud poll in ACTIVE (i.e. mow start, or the moment we fall
    back to cloud mid-mow — exactly when close monitoring matters).
    """
    table = (
        _MQTT_NEW_POLL_INTERVAL
        if not Transport._version_is_rate_limited(handle.firmware_version)  # noqa: SLF001
        else _MQTT_POLL_INTERVAL
    )
    mode = handle.device_mode()
    base = table[mode]
    now = time.monotonic()

    if mode is not _DeviceMode.ACTIVE:
        handle._mowing_active_since = 0.0  # noqa: SLF001 — reset resume window once mowing ends
        if mode in (_DeviceMode.DOCKED_CHARGING, _DeviceMode.DOCKED_FULL):
            if handle._docked_since == 0.0:  # noqa: SLF001 — stamp dock entry (fast-cooldown/reconnect window)
                handle._docked_since = now  # noqa: SLF001
            ble = handle._transports.get(TransportType.BLE)  # noqa: SLF001
            ble_reason = None if ble is None else ble.usable_reason
            recently_docked = handle._recently_docked()  # noqa: SLF001
            # Dense reconnect only within the dock-settling window (bounds cloud spend if
            # BLE ever stays usable-but-not-connecting); after it, relax to base cadence.
            if recently_docked and ble is not None and not ble.is_connected and ble.is_usable:
                _logger.debug(
                    "poll_interval [%s]: DOCKED (recent) + BLE disconnected+usable -> %.0fs (reason=docked-ble-reconnect)",
                    handle.device_name,
                    _DOCKED_BLE_RECONNECT_INTERVAL,
                )
                return _DOCKED_BLE_RECONNECT_INTERVAL, "docked-ble-reconnect"
            _logger.debug(
                "poll_interval [%s]: DOCKED base=%.0f recently_docked=%s ble_connected=%s ble_usable_reason=%s -> base",
                handle.device_name,
                base,
                recently_docked,
                None if ble is None else ble.is_connected,
                ble_reason,
            )
        else:
            handle._docked_since = 0.0  # noqa: SLF001 — not docked (IDLE/paused): clear settling window
        return base, f"base:{mode.name.lower()}"

    # ACTIVE (mowing/returning/paused): not docked — clear the dock-settling window.
    handle._docked_since = 0.0  # noqa: SLF001
    # The resume window is armed only by a fresh WORKING (actively mowing) start — NOT by
    # RETURNING / PAUSE / CHARGING_PAUSE, which device_mode() also buckets into ACTIVE.  A
    # long pause otherwise kept _mowing_active_since set for hours so the window never
    # re-armed on the next mow (observed 2026-07-27: since_active=51253s → base not resume).
    # Also gated on ``not ble_stream_active`` so it starts at the cloud-fallback moment
    # (or mow start if mowing on cloud from the outset), not while a BLE stream is feeding.
    if not handle._is_working():  # noqa: SLF001
        handle._mowing_active_since = 0.0  # noqa: SLF001 — only a fresh WORKING start arms the window
    elif handle._mowing_active_since == 0.0 and not handle.ble_stream_active:  # noqa: SLF001
        handle._mowing_active_since = now  # noqa: SLF001

    interval = base
    reason = "base:active"
    if now - handle._mowing_active_since < _ACTIVE_RESUME_DENSE_WINDOW:  # noqa: SLF001
        if _ACTIVE_RESUME_DENSE_INTERVAL < interval:
            interval, reason = _ACTIVE_RESUME_DENSE_INTERVAL, "resume-window"
    if handle._is_returning() and _ACTIVE_RETURNING_INTERVAL < interval:  # noqa: SLF001
        interval, reason = _ACTIVE_RETURNING_INTERVAL, "returning"
    battery = handle.battery_percent
    if battery is not None and battery < _ACTIVE_LOW_BATTERY_THRESHOLD:
        if _ACTIVE_LOW_BATTERY_INTERVAL < interval:
            interval, reason = _ACTIVE_LOW_BATTERY_INTERVAL, "low-battery"
    progress = handle.work_progress
    if progress is not None and progress > _ACTIVE_NEAR_END_PROGRESS_THRESHOLD:
        if _ACTIVE_NEAR_END_INTERVAL < interval:
            interval, reason = _ACTIVE_NEAR_END_INTERVAL, "near-end"
    mins_to_nonwork = _minutes_until_nonwork_start(handle)
    if mins_to_nonwork is not None and mins_to_nonwork <= _ACTIVE_PRE_NONWORK_WINDOW_MIN:
        if _ACTIVE_PRE_NONWORK_INTERVAL < interval:
            interval, reason = _ACTIVE_PRE_NONWORK_INTERVAL, "pre-nonwork"
    # Diagnostic: log the raw inputs behind the ACTIVE decision so a surprising
    # overlay is explainable from the log alone rather than inferred (2026-07-26
    # investigation).  nonwork_raw is the raw device string being parsed — logged
    # to confirm whether a wrong minutes-to-non-work is a parse/format issue or a
    # stale/divergent value vs the HA non-work sensor.
    try:
        nonwork_raw = handle.snapshot.raw.non_work_hours.start_time  # type: ignore[union-attr]
    except AttributeError:
        nonwork_raw = None
    _logger.debug(
        "poll_interval [%s]: ACTIVE base=%.0f battery=%s progress=%s "
        "nonwork_start_raw=%r mins_to_nonwork=%s since_active=%.0fs -> %.0fs (reason=%s)",
        handle.device_name,
        base,
        battery,
        progress,
        nonwork_raw,
        None if mins_to_nonwork is None else round(mins_to_nonwork),
        now - handle._mowing_active_since,  # noqa: SLF001
        interval,
        reason,
    )
    return interval, reason


def _minutes_until_nonwork_start(handle: DeviceHandle) -> float | None:
    """Wall-clock minutes from now until the device's non-work window starts.

    Reads the device-reported non-work start (``non_work_hours.start_time``) and
    returns the minutes until its next occurrence in local time (0..1439).  ``None``
    when unset/unparseable.  Drives the pre-non-work dense-cadence overlay so Luba's
    forced return-to-dock at the deadline is polled closely.

    NOTE the raw string is **minutes-from-midnight**, NOT ``"HHMM"`` — e.g. ``"1230"``
    is 20:30 (1230 min), not 12:30.  This mirrors the HA sensor's ``parse_time_string``
    (``time(total // 60 % 24, total % 60)``); parsing it as HHMM put the overlay 8h
    off (2026-07-26 investigation — surfaced as a suspected timezone issue).
    """
    try:
        raw = handle.snapshot.raw.non_work_hours.start_time  # type: ignore[union-attr]
    except AttributeError:
        return None
    if not raw:
        return None
    try:
        start_mod = int(raw) % 1440  # raw is minutes-from-midnight
    except (TypeError, ValueError):
        return None
    now = datetime.now()
    now_mod = now.hour * 60 + now.minute
    return float((start_mod - now_mod) % 1440)


async def _record_and_sleep(handle: DeviceHandle, seconds: float, reason: str) -> bool:
    """Record the loop's current sleep on the handle (for the sensor) and wait.

    Stashes ``(seconds, reason)`` so the poll-cadence diagnostic sensor can surface
    exactly how long ``mqtt_activity_loop`` is sleeping and why — including defers
    (``ble-stream-active``, ``no-usable-transport``, ``rate-limited``, …), not just
    the ``poll_interval`` overlay choice.  Then defers to ``sleep_or_rearm`` and
    returns ``True`` if a user command rearmed the loop early.
    """
    handle._last_poll_sleep_seconds = seconds  # noqa: SLF001
    handle._last_poll_reason = reason  # noqa: SLF001
    _logger.debug("poll_loop [%s]: sleeping %.0fs (reason=%s)", handle.device_name, seconds, reason)
    return await handle.sleep_or_rearm(seconds)


async def mqtt_activity_loop(handle: DeviceHandle) -> None:
    """Periodic one-shot report-poll loop (MQTT-side cadence driver).

    Sends ``request_iot_sys(count=1)`` via the best available transport
    (BLE if connected, MQTT otherwise) once the device has been silent for
    longer than the per-mode interval defined in ``_MQTT_POLL_INTERVAL``:

    * **ACTIVE**         — 20 min (mowing/returning).
    * **DOCKED_CHARGING** — 30 min (docked, battery < 100%).
    * **DOCKED_FULL**    — 60 min (docked, battery 100%).
    * **IDLE**           — 15 min (paused/locked/lost).

    While ``handle.ble_stream_active`` is True the BLE polling loop is feeding
    a continuous count=0 stream and this loop defers entirely; the BLE
    availability handler clears the flag and rearms us on disconnect.

    The timer resets on either incoming device data or a sent poll, so a
    device that doesn't respond is polled at most once per interval.

    The loop is interruptible: ``record_user_command`` sets ``_rearm_event``
    to wake an in-progress sleep early for immediate re-evaluation.
    """
    last_poll_sent_at: float = 0.0

    while not handle._stopping:  # noqa: SLF001
        interval, interval_reason = poll_interval(handle)

        # While the BLE polling loop owns a continuous stream, this loop
        # has nothing useful to do — fresh state is arriving over BLE.
        if handle.ble_stream_active:
            await _record_and_sleep(handle, _BLE_MODE_RECHECK_INTERVAL, "ble-stream-active")
            continue

        # No usable transport (cloud reported device offline + no BLE,
        # BLE in cooldown + no MQTT, or nothing registered).  Skip the
        # poll attempt — ``_rearm_event`` fires on BLE state changes and
        # ``mqtt_reported_offline`` clears on the next inbound MQTT frame,
        # so both natural recovery signals already wake us.
        if not handle.has_usable_transport:
            await _record_and_sleep(
                handle,
                interval,
                f"no-usable-transport(mqtt_offline={handle._availability.mqtt_reported_offline})",  # noqa: SLF001
            )
            continue

        # Timer: the later of "last fresh REPORT data" and "last poll sent".
        # Use last_report_at (bumped only on a parsed LubaMsg report, from BLE or cloud)
        # rather than transport last_received_monotonic (bumped on ANY inbound frame,
        # incl. thing.event notifications) — a junk notification must NOT defer the poll
        # and delay a state transition (e.g. docked) by a full interval (observed 2026-07-29:
        # a code-1307 notification pushed RPT_START ~3 min late while returning).
        # Including last_poll_sent_at prevents spam when the device doesn't respond.
        last_recv = handle.last_report_at
        last_activity = max(last_recv, last_poll_sent_at)
        wait = interval - (time.monotonic() - last_activity)

        if wait > 0:
            if await _record_and_sleep(handle, wait, interval_reason):
                continue  # rearmed by user command — re-evaluate immediately
            last_recv = handle.last_report_at
            last_activity = max(last_recv, last_poll_sent_at)
            if time.monotonic() - last_activity < interval:
                continue

        if not handle._transports:  # noqa: SLF001
            await _record_and_sleep(handle, interval, "no-transports")
            continue

        # Back off if MQTT is rate-limited and no BLE transport is connected.
        mqtt: Transport | None = None
        for tt in (TransportType.CLOUD_ALIYUN, TransportType.CLOUD_MAMMOTION):
            t = handle._transports.get(tt)  # noqa: SLF001
            if t is not None:
                mqtt = t
                break
        if mqtt is not None and mqtt.is_send_blocked(handle.firmware_version):
            ble = handle._transports.get(TransportType.BLE)  # noqa: SLF001
            if ble is None or not ble.is_connected:
                # Back off only until sends are actually available again (the rolling
                # window sliding under the limit, or the cloud ban expiring) so the loop
                # resumes promptly instead of sleeping a flat _RATE_LIMITED_BACKOFF.
                # Floored at 60 s to avoid a tight retry loop at the boundary and capped
                # so a never-set release time can't park the loop forever.
                backoff = min(_RATE_LIMITED_BACKOFF, max(60.0, mqtt.seconds_until_send_available()))
                await _record_and_sleep(handle, backoff, "rate-limited-no-ble")
                continue

        if handle.queue.is_saga_active or handle.in_no_request_mode():
            await _record_and_sleep(handle, interval, "saga-or-no-request")
            continue

        _logger.debug(
            "poll_loop [%s]: %.0fs since last activity — sending one-shot poll (interval=%.0fs, reason=%s)",
            handle.device_name,
            time.monotonic() - last_activity,
            interval,
            interval_reason,
        )
        last_poll_sent_at = time.monotonic()
        await handle._send_one_shot_report()  # noqa: SLF001
