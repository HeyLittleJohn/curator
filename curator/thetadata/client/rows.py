"""Typed rows yielded by the Options Standard client.

Field names follow the Theta Terminal v3 CSV headers. Extra columns are ignored.
Empty cells are treated as missing so optional greeks stay unset.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any

from pydantic import AliasChoices, BaseModel, BeforeValidator, ConfigDict, Field, model_validator


class OptionRight(str, Enum):
    """Option right, normalized from terminal values such as ``C`` or ``call``."""

    CALL = "CALL"
    PUT = "PUT"


def _parse_right(value: Any) -> str:
    """Normalize a terminal right value.

    Args:
        value: Cell text or enum.

    Returns:
        ``CALL`` or ``PUT``.

    Raises:
        ValueError: The value is not a call or a put.
    """
    if isinstance(value, OptionRight):
        return value.value
    token = str(value).strip().upper()
    if token in {"C", "CALL"}:
        return OptionRight.CALL.value
    if token in {"P", "PUT"}:
        return OptionRight.PUT.value
    raise ValueError("unknown option right")


def _parse_date(value: Any) -> date:
    """Parse a terminal date cell.

    Args:
        value: ``YYYY-MM-DD``, ``YYYYMMDD``, or a date.

    Returns:
        The calendar date.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text[:10])


def _parse_datetime(value: Any) -> datetime:
    """Parse a terminal timestamp cell.

    Args:
        value: ISO-8601 text or a datetime.

    Returns:
        The timestamp, kept naive when the terminal omits a timezone.
    """
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).strip())


def _parse_decimal(value: Any) -> Decimal:
    """Parse a price or greek cell.

    Args:
        value: Numeric text or number.

    Returns:
        The exact decimal value.
    """
    return Decimal(str(value))


def _parse_int(value: Any) -> int:
    """Parse an integer cell.

    Args:
        value: Integer text or number.

    Returns:
        The integer value.
    """
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer cell")
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return int(text)


RightField = Annotated[OptionRight, BeforeValidator(_parse_right)]
DateField = Annotated[date, BeforeValidator(_parse_date)]
DateTimeField = Annotated[datetime, BeforeValidator(_parse_datetime)]
DecimalField = Annotated[Decimal, BeforeValidator(_parse_decimal)]
IntField = Annotated[int, BeforeValidator(_parse_int)]


class ThetaRow(BaseModel):
    """Base row. Unknown columns and blank cells are dropped."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def drop_blank_cells(cls, data: Any) -> Any:
        """Drop blank strings so optional fields stay at their defaults.

        Args:
            data: A row mapping, or some other input pydantic will reject.

        Returns:
            The same input, with blank string cells removed when it is a dict.
        """
        if not isinstance(data, dict):
            return data
        cleaned: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, str):
                value = value.strip()
                if value == "":
                    continue
            cleaned[str(key).strip()] = value
        return cleaned


class ExpirationRow(ThetaRow):
    """One expiration from ``option/list/expirations``."""

    symbol: str
    expiration: DateField


class StrikeRow(ThetaRow):
    """One strike from ``option/list/strikes``."""

    symbol: str
    strike: DecimalField


class ContractRow(ThetaRow):
    """One contract from ``option/list/contracts``."""

    symbol: str
    expiration: DateField
    strike: DecimalField
    right: RightField


class QuoteRow(ThetaRow):
    """One NBBO quote from ``option/history/quote``."""

    symbol: str
    expiration: DateField
    strike: DecimalField
    right: RightField
    timestamp: DateTimeField
    bid: DecimalField
    ask: DecimalField
    bid_size: IntField | None = None
    bid_exchange: IntField | None = None
    bid_condition: IntField | None = None
    ask_size: IntField | None = None
    ask_exchange: IntField | None = None
    ask_condition: IntField | None = None


class OhlcRow(ThetaRow):
    """One OHLC bar from ``option/history/ohlc``."""

    symbol: str
    expiration: DateField
    strike: DecimalField
    right: RightField
    timestamp: DateTimeField
    open: DecimalField
    high: DecimalField
    low: DecimalField
    close: DecimalField
    volume: IntField
    count: IntField | None = None
    vwap: DecimalField | None = None


class OpenInterestRow(ThetaRow):
    """One open-interest print from ``option/history/open_interest``."""

    symbol: str
    expiration: DateField
    strike: DecimalField
    right: RightField
    timestamp: DateTimeField
    open_interest: IntField


class EodRow(ThetaRow):
    """One end-of-day report from ``option/history/eod``."""

    symbol: str
    expiration: DateField
    strike: DecimalField
    right: RightField
    open: DecimalField
    high: DecimalField
    low: DecimalField
    close: DecimalField
    volume: IntField
    count: IntField | None = None
    created: DateTimeField | None = None
    last_trade: DateTimeField | None = None
    bid_size: IntField | None = None
    bid_exchange: IntField | None = None
    bid: DecimalField | None = None
    bid_condition: IntField | None = None
    ask_size: IntField | None = None
    ask_exchange: IntField | None = None
    ask: DecimalField | None = None
    ask_condition: IntField | None = None


class GreeksRow(ThetaRow):
    """One first-order greek row, including IV and the underlying price.

    The terminal calculates these from the option quote. ``underlying_price``
    is stored with the greek and is not a spot source.
    """

    symbol: str
    expiration: DateField
    strike: DecimalField
    right: RightField
    timestamp: DateTimeField
    bid: DecimalField | None = None
    ask: DecimalField | None = None
    delta: DecimalField | None = None
    gamma: DecimalField | None = None
    theta: DecimalField | None = None
    vega: DecimalField | None = None
    rho: DecimalField | None = None
    epsilon: DecimalField | None = None
    lambda_: DecimalField | None = Field(
        default=None,
        validation_alias=AliasChoices("lambda", "lambda_"),
    )
    implied_vol: DecimalField | None = None
    iv_error: DecimalField | None = None
    underlying_timestamp: DateTimeField | None = None
    underlying_price: DecimalField | None = None
