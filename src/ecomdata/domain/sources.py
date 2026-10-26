"""The one registry of data sources.

A :class:`SourceSpec` is everything the platform knows about one source as
data: its identity, where its pages live, how to fetch them, which extractor
finds the rows and which normaliser turns them into offer observations, its
currency, which public counters it publishes with trusted semantics, how its
categories are discovered, and which fields are personal data and must be
dropped before a record is written.

Extractor, normaliser and discovery are named by dotted path
(``package.module:Attribute`` or ``package.module``), so this module imports
nothing outside the domain. :meth:`SourceRegistry.resolve` loads them on
demand; a test can also pass the object itself.

Adding a source: write its spec here, an extractor under
``ecomdata.ingestion.sources.<code>``, a normaliser under
``ecomdata.processing.normalize.sources.<code>``, and register the spec.
"""
from __future__ import annotations

import importlib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from ecomdata.domain.validation import currency_code, http_url, require_text

HTTP = "http"
BROWSER = "browser"
API = "api"
FETCHERS = (HTTP, BROWSER, API)

B2C_MARKETPLACE = "b2c_marketplace"
C2C_CLASSIFIED = "c2c_classified"
RETAILER = "retailer"
SOURCE_TYPES = (B2C_MARKETPLACE, C2C_CLASSIFIED, RETAILER)


@dataclass(frozen=True)
class CounterSemantic:
    """A public counter whose deltas may be trusted, and under which version."""

    marketplace: str
    field_name: str
    enabled: bool
    monotonic_expected: bool
    semantic_version: str


@dataclass(frozen=True)
class SourceSpec:
    code: str
    marketplace_id: str
    name: str
    base_url: str
    fetcher: str
    extractor: Any
    normalizer: Any
    currency: str
    source_type: str = B2C_MARKETPLACE
    country: str = "VN"
    locale: str = "vi-VN"
    platform_type: str = "marketplace"
    # Bump when the extractor's output changes shape; carried on every record.
    extractor_version: str = "1"
    discovery: Any = None
    # Listing pages beyond the first carry different rows (False for Tiki,
    # whose ?page=2 serves page 1 again).
    paginated: bool = False
    # Targets crawled when none is named (``<CODE>_CATEGORIES`` overrides them).
    default_targets: tuple[str, ...] = ()
    # Politeness floor between two requests to this source, in seconds. The
    # configured delay can be larger, never smaller.
    min_delay_seconds: float = 2.0
    # Public counters with trusted semantics: (field, monotonic expected).
    counters: tuple[tuple[str, bool], ...] = ()
    counter_semantics_version: str = "v1"
    rating_scale: str = "5"
    # Personal data. Top-level keys dropped from every listing row, and from
    # the page context, before a record is written.
    pii_row_fields: tuple[str, ...] = ()
    pii_context_fields: tuple[str, ...] = ()
    # Browser fetcher only: which responses to keep, what the page must show
    # before it is read, and how a block or challenge page looks.
    capture_patterns: tuple[str, ...] = ()
    wait_for: str | None = None
    blocked_patterns: tuple[str, ...] = ()
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", require_text(self.code, "code").lower())
        require_text(self.marketplace_id, "marketplace_id")
        require_text(self.name, "name")
        http_url(self.base_url, "base_url")
        if self.fetcher not in FETCHERS:
            raise ValueError(f"fetcher must be one of {FETCHERS}: {self.fetcher!r}")
        if self.source_type not in SOURCE_TYPES:
            raise ValueError(f"source_type must be one of {SOURCE_TYPES}: {self.source_type!r}")
        object.__setattr__(self, "currency", currency_code(self.currency))
        if self.min_delay_seconds < 0:
            raise ValueError("min_delay_seconds must not be negative")
        if self.extractor is None or self.normalizer is None:
            raise ValueError(f"{self.code}: extractor and normalizer are required")

    @property
    def adapter_version(self) -> str:
        """What raw artifacts and records carry as the producing code's version."""
        return f"{self.code}-{self.fetcher}-extractor-v{self.extractor_version}"

    def counter_semantics(self) -> tuple[CounterSemantic, ...]:
        return tuple(CounterSemantic(self.code, name, True, monotonic, self.counter_semantics_version)
                     for name, monotonic in self.counters)

    def counter_semantic(self, field_name: str) -> CounterSemantic | None:
        return next((c for c in self.counter_semantics() if c.field_name == field_name), None)


def resolve(target: Any) -> Any:
    """Load ``package.module:Attribute`` or ``package.module``; objects pass through."""
    if not isinstance(target, str):
        return target
    module_name, _, attribute = target.partition(":")
    module = importlib.import_module(module_name)
    return getattr(module, attribute) if attribute else module


class SourceRegistry:
    """Source code -> :class:`SourceSpec`. The only place a source is declared."""

    def __init__(self, specs: Iterable[SourceSpec] = ()) -> None:
        self._specs: dict[str, SourceSpec] = {}
        for spec in specs:
            self.register(spec)

    def register(self, spec: SourceSpec) -> SourceSpec:
        if not isinstance(spec, SourceSpec):
            raise TypeError("spec must be a SourceSpec")
        if spec.code in self._specs:
            raise ValueError(f"source already registered: {spec.code}")
        if any(s.marketplace_id == spec.marketplace_id for s in self._specs.values()):
            raise ValueError(f"marketplace_id already registered: {spec.marketplace_id}")
        self._specs[spec.code] = spec
        return spec

    def with_spec(self, spec: SourceSpec) -> SourceRegistry:
        """A copy with one more source; the registry it was made from is unchanged."""
        return SourceRegistry([*self._specs.values(), spec])

    def get(self, code: str) -> SourceSpec:
        try:
            return self._specs[str(code).strip().lower()]
        except KeyError:
            raise ValueError(f"unknown source: {code!r} (registered: {', '.join(self.codes())})") from None

    def find(self, code: str) -> SourceSpec | None:
        return self._specs.get(str(code).strip().lower())

    def by_marketplace_id(self, marketplace_id: str) -> SourceSpec:
        for spec in self._specs.values():
            if spec.marketplace_id == marketplace_id:
                return spec
        raise ValueError(f"unknown marketplace_id: {marketplace_id!r}")

    def codes(self) -> tuple[str, ...]:
        return tuple(sorted(self._specs))

    def __iter__(self) -> Iterator[SourceSpec]:
        return iter(sorted(self._specs.values(), key=lambda s: s.code))

    def __contains__(self, code: object) -> bool:
        return isinstance(code, str) and code.strip().lower() in self._specs

    def __len__(self) -> int:
        return len(self._specs)

    def extractor(self, code: str) -> Any:
        return resolve(self.get(code).extractor)

    def normalizer(self, code: str) -> Any:
        return resolve(self.get(code).normalizer)

    def discovery(self, code: str) -> Any:
        spec = self.get(code)
        if spec.discovery is None:
            raise ValueError(f"source {code!r} declares no discovery")
        return resolve(spec.discovery)


TIKI = SourceSpec(
    code="tiki",
    marketplace_id="marketplace-tiki",
    name="Tiki",
    base_url="https://tiki.vn",
    fetcher=HTTP,
    extractor="ecomdata.ingestion.sources.tiki.extractor:TikiExtractor",
    normalizer="ecomdata.processing.normalize.sources.tiki",
    discovery="ecomdata.ingestion.sources.tiki.discovery:TikiSitemapDiscovery",
    currency="VND",
    paginated=False,
    # Nine broad top-level categories as <slug>/c<id>, ids from the sitemap (2026-10-25).
    default_targets=(
        "laptop-may-vi-tinh-linh-kien/c1846",
        "nha-sach-tiki/c8322",
        "dien-gia-dung/c1882",
        "lam-dep-suc-khoe/c1520",
        "thoi-trang-nu/c931",
        "thoi-trang-nam/c915",
        "bach-hoa-online/c4384",
        "dien-thoai-may-tinh-bang/c1789",
        "cross-border-hang-quoc-te/c17166",
    ),
    min_delay_seconds=2.0,
    counters=(("rating_count", True), ("review_count", True), ("sold_count", True)),
    extractor_version="1",
)

LAZADA = SourceSpec(
    code="lazada",
    marketplace_id="marketplace-lazada",
    name="Lazada",
    base_url="https://www.lazada.vn",
    fetcher=BROWSER,
    extractor="ecomdata.ingestion.sources.lazada.extractor:LazadaExtractor",
    normalizer="ecomdata.processing.normalize.sources.lazada",
    discovery="ecomdata.ingestion.sources.lazada.discovery:LazadaConfiguredDiscovery",
    currency="VND",
    paginated=True,
    min_delay_seconds=5.0,
    # itemSoldCntShow is a rounded display string ("1.2K"), not a counter.
    counters=(("review_count", True),),
    extractor_version="1",
    # The anonymous visitor identifiers Lazada echoes into the page state.
    pii_context_fields=("mainInfo.userId", "mainInfo.anonUid"),
    capture_patterns=(r"[?&]ajax=true",),
    blocked_patterns=(r"/_____tmd_____/punish", r"punish\?x5secdata", r"baxia-punish", r"captcha", r"slide to verify"),
)

SOURCES = SourceRegistry([TIKI, LAZADA])
