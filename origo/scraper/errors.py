from __future__ import annotations

from dataclasses import dataclass, field

ScraperErrorCode = str


def _empty_str_dict() -> dict[str, str]:
    return {}


@dataclass(frozen=True)
class ScraperError(RuntimeError):
    code: ScraperErrorCode
    message: str
    details: dict[str, str] = field(default_factory=_empty_str_dict)

    def __post_init__(self) -> None:
        if self.code.strip() == '':
            raise ValueError('ScraperError.code must be non-empty')
        if self.message.strip() == '':
            raise ValueError('ScraperError.message must be non-empty')
        super().__init__(self.message)

    def to_dict(self) -> dict[str, str]:
        payload = {'code': self.code, 'message': self.message}
        payload.update(self.details)
        return payload


def as_scraper_error(
    *,
    code: ScraperErrorCode,
    message: str,
    details: dict[str, str] | None = None,
    cause: Exception | None = None,
) -> ScraperError:
    detail_payload = details if details is not None else {}
    if cause is not None:
        detail_payload = {
            **detail_payload,
            'cause_type': type(cause).__name__,
            'cause_message': str(cause),
        }
    return ScraperError(
        code=code,
        message=message,
        details=detail_payload,
    )
