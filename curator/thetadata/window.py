"""Strike window around the chain's own at-the-money strike.

ATM is chosen per trade date and expiration from that expiration's listed
strikes. It does not use an index quote, and it does not use the greeks
``underlying_price`` field. ``underlying_price`` is stored with the greek and
is not a spot source.

Method, in order:

1. Listed strikes come from ``option/list/strikes`` for that expiration. When
   the terminal returns no strike list, the loader keeps every strike in the
   history response instead of guessing a center.
2. The open is the earliest timestamp in the history response. Theta Terminal
   sends bars in timestamp order, so that first timestamp is the session open.
3. The premium at the open is the quote or greek mid ``(bid + ask) / 2``. For
   OHLC it is the bar's ``open``. Strikes missing either side are skipped.
4. Each two-sided strike produces an undiscounted put-call forward
   ``strike + call_premium - put_premium``. The session forward is the median
   of those forwards. An even count averages the two middle values.
5. ATM is the listed strike closest to that forward. A tie keeps the lower
   strike.
6. ``strike_range`` is how many listed strikes to keep on each side of ATM,
   counting positions in the sorted chain. The default is 125. ``None`` keeps
   the full chain and does not price it.
7. When the open bar has no two-sided strike, ATM is the lower median of the
   listed strikes (the chain center).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from curator.thetadata.client.rows import GreeksRow, OhlcRow, QuoteRow
from curator.thetadata.identity import RIGHT_CALL, RIGHT_PUT

DEFAULT_STRIKE_RANGE = 125

METHOD_PARITY = "put_call_parity"
METHOD_CENTER = "chain_center"
METHOD_UNLIMITED = "unlimited"

_UNLIMITED = frozenset({"all", "unlimited", "*"})
PricedRow = QuoteRow | OhlcRow | GreeksRow


class StrikeWindowError(ValueError):
    """The strike window cannot be built."""


@dataclass(frozen=True)
class StrikeWindow:
    """Strikes kept for one session and expiration.

    Args:
        atm: At-the-money listed strike. ``None`` when the chain is unlimited.
        strikes: Strikes the loader stores.
        forward: Median put-call forward. ``None`` for the chain center and
            for an unlimited chain.
        method: ``put_call_parity``, ``chain_center``, or ``unlimited``.
    """

    atm: Decimal | None
    strikes: frozenset[Decimal]
    forward: Decimal | None
    method: str


def parse_strike_range(value: str) -> int | None:
    """Parse a strike-window width.

    Args:
        value: ``125``, ``0``, ``all``, ``unlimited``, or ``*``.

    Returns:
        Strikes on each side of ATM, or ``None`` for the full chain.

    Raises:
        StrikeWindowError: The token is not a width or an unlimited alias.
    """
    token = value.strip().lower()
    if token in _UNLIMITED:
        return None
    if not token.isdecimal():
        raise StrikeWindowError("strike_range must be a non-negative integer or 'all'")
    return int(token)


def open_premium(row: PricedRow) -> Decimal | None:
    """Return the open premium used to locate ATM.

    Quote and greek rows use the bid/ask mid. OHLC uses the bar open. A greek
    ``underlying_price`` is ignored on purpose.

    Args:
        row: One open-bar row.

    Returns:
        The premium, or ``None`` when a quote or greek is missing a side.
    """
    if isinstance(row, OhlcRow):
        return row.open
    if row.bid is None or row.ask is None:
        return None
    return (row.bid + row.ask) / 2


def select_strike_window(
    listed: list[Decimal] | tuple[Decimal, ...],
    anchor: list[tuple[Decimal, str, Decimal]] | tuple[tuple[Decimal, str, Decimal], ...],
    strike_range: int | None,
) -> StrikeWindow:
    """Choose the strikes to store for one session and expiration.

    Args:
        listed: Chain strikes. Duplicates collapse. Order does not matter.
        anchor: ``(strike, right, premium)`` tuples from the open bar.
            ``right`` is ``call`` or ``put``.
        strike_range: Listed strikes on each side of ATM. ``None`` keeps the
            whole chain. ``0`` keeps only ATM.

    Returns:
        The window. ``strikes`` is empty only when ``listed`` is empty and the
        caller asked for a bounded window; callers with an empty chain should
        keep the history response instead.

    Raises:
        StrikeWindowError: ``strike_range`` is negative, or a bounded window
            was requested for an empty chain.
    """
    ordered = tuple(sorted(set(listed)))
    if strike_range is None:
        return StrikeWindow(atm=None, strikes=frozenset(ordered), forward=None, method=METHOD_UNLIMITED)
    if strike_range < 0:
        raise StrikeWindowError("strike_range must be >= 0")
    if not ordered:
        raise StrikeWindowError("strike window needs the listed chain")
    forward = _forward(anchor)
    if forward is None:
        atm = _center(ordered)
        return StrikeWindow(atm=atm, strikes=_around(ordered, atm, strike_range), forward=None, method=METHOD_CENTER)
    atm = _nearest(ordered, forward)
    return StrikeWindow(
        atm=atm,
        strikes=_around(ordered, atm, strike_range),
        forward=forward,
        method=METHOD_PARITY,
    )


def anchor_from_rows(
    rows: list[PricedRow] | tuple[PricedRow, ...],
) -> tuple[datetime | None, list[tuple[Decimal, str, Decimal]]]:
    """Build the open-bar anchor from already parsed rows.

    Args:
        rows: Rows from one history response, any timestamp order.

    Returns:
        The earliest timestamp, and ``(strike, right, premium)`` for rows on
        that timestamp that have a premium. Rights are ``call`` or ``put``.
    """
    if not rows:
        return None, []
    first = min(row.timestamp for row in rows)
    anchor: list[tuple[Decimal, str, Decimal]] = []
    for row in rows:
        if row.timestamp != first:
            continue
        premium = open_premium(row)
        if premium is None:
            continue
        right = row.right.value.lower()
        if right not in {RIGHT_CALL, RIGHT_PUT}:
            continue
        anchor.append((row.strike, right, premium))
    return first, anchor


def _forward(anchor: list[tuple[Decimal, str, Decimal]] | tuple[tuple[Decimal, str, Decimal], ...]) -> Decimal | None:
    """Return the median put-call forward for one open bar.

    Args:
        anchor: Open premiums. The last premium for a strike and right wins.

    Returns:
        The median forward, or ``None`` when no strike has both sides.
    """
    calls: dict[Decimal, Decimal] = {}
    puts: dict[Decimal, Decimal] = {}
    for strike, right, premium in anchor:
        if right == RIGHT_CALL:
            calls[strike] = premium
        elif right == RIGHT_PUT:
            puts[strike] = premium
    forwards = [strike + calls[strike] - puts[strike] for strike in calls.keys() & puts.keys()]
    if not forwards:
        return None
    return _median(forwards)


def _median(values: list[Decimal]) -> Decimal:
    """Return the median of a non-empty decimal list.

    Args:
        values: Forward estimates.

    Returns:
        The middle value, or the average of the two middle values.
    """
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _center(ordered: tuple[Decimal, ...]) -> Decimal:
    """Return the lower median listed strike.

    Args:
        ordered: Unique strikes, ascending, non-empty.

    Returns:
        The middle strike. An even count keeps the lower of the two middle strikes.
    """
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return ordered[mid - 1]


def _nearest(ordered: tuple[Decimal, ...], forward: Decimal) -> Decimal:
    """Return the listed strike closest to ``forward``.

    Args:
        ordered: Unique strikes, ascending, non-empty.
        forward: Session forward from put-call parity.

    Returns:
        The nearest strike. A tie keeps the lower strike.
    """
    best = ordered[0]
    best_distance = abs(ordered[0] - forward)
    for strike in ordered[1:]:
        distance = abs(strike - forward)
        if distance < best_distance:
            best = strike
            best_distance = distance
    return best


def _around(ordered: tuple[Decimal, ...], atm: Decimal, strike_range: int) -> frozenset[Decimal]:
    """Return ATM and up to ``strike_range`` listed strikes on each side.

    Args:
        ordered: Unique strikes, ascending.
        atm: A strike that is present in ``ordered``.
        strike_range: Extra strikes on each side.

    Returns:
        The inclusive index window.
    """
    index = ordered.index(atm)
    start = max(0, index - strike_range)
    stop = min(len(ordered), index + strike_range + 1)
    return frozenset(ordered[start:stop])
