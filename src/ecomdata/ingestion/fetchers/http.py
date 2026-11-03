"""Plain HTTP: one GET, one document part."""
from __future__ import annotations

from ecomdata.domain.acquisition import ROLE_DOCUMENT, FetchBundle, FetchPart, ListingPageRequest
from ecomdata.infra.http import HttpClient
from ecomdata.ingestion.fetchers.base import SourceFetcher


class HttpFetcher(SourceFetcher):
    def __init__(self, *args, client: HttpClient, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.client = client

    def fetch(self, request: ListingPageRequest, url: str) -> FetchBundle:
        response = self.client.get(url)
        part = FetchPart(
            url=response.url,
            http_status=response.status_code,
            content_type=response.header("Content-Type"),
            body=response.content,
            role=ROLE_DOCUMENT,
            elapsed_ms=response.elapsed_ms,
        )
        return FetchBundle(
            request_url=url, fetched_at=self.clock(), parts=(part,), retry_after=response.header("Retry-After")
        )

    def close(self) -> None:
        self.client.close()
