from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any
from urllib.parse import quote
from urllib.parse import urljoin
from urllib.parse import urlsplit

import bs4
import requests
from pydantic import BaseModel


from onyx.file_processing.html_utils import format_document_soup
from onyx.utils.logger import setup_logger
from onyx.utils.retry_wrapper import retry_builder

logger = setup_logger()

_DEFAULT_PAGE_SIZE = 500
_TIMEOUT_SECONDS = 30


class XWikiError(Exception):
    """Base error for XWiki interactions."""


class XWikiAuthenticationError(XWikiError):
    """Raised when authentication fails."""


class XWikiUnexpectedResponse(XWikiError):
    """Raised when the XWiki API returns unexpected data."""


def _encode_segments(segments: Sequence[str]) -> str:
    return "/".join(quote(part, safe="") for part in segments)


def _encode_name(value: str) -> str:
    return quote(value, safe="")


@dataclass(frozen=True)
class XWikiPage:
    id: str  # e.g., "xwiki:Main.WebHome" or "subwiki:Main.WebHome"
    full_name: str  # pageFullName
    page_url: str  # REST href for rel=page (non-translation)
    modified_ms: int  # epoch ms
    content: str
    attachments: list[XWikiAttachment]


class XWikiAttachment(BaseModel):
    name: str
    size: int | None = None
    mime_type: str | None = None
    download_path: str
    version: str | None = None
    digest: str | None = None


def _build_session(username: str, password: str) -> requests.Session:
    session = requests.Session()
    session.auth = (username, password)
    return session


def _encoded_space_path(spaces: Sequence[str]) -> str:
    return _encode_segments(spaces)


def _encoded_page(page: str) -> str:
    return _encode_name(page)


def _build_modified_filter(since_ms: int | None) -> str:
    if since_ms is None:
        return ""
    since_iso = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return f"date:[{since_iso} TO *]"


class XWikiClient:
    """Lightweight client for interacting with XWiki REST and SOLR endpoints."""

    def __init__(
            self,
            *,
            base_url: str,
            username: str,
            password: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = _build_session(username, password)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @retry_builder(tries=3, delay=1, backoff=2, max_delay=10)
    def _request(
            self,
            method: str,
            url: str,
            *,
            headers: dict[str, str] | None = None,
            params: dict[str, Any] | None = None,
            allow_status: set[int] | None = None,
            data: Any = None,
            json: Any = None,
    ) -> requests.Response:
        request_url = (
            url
            if url.startswith("http")
            else urljoin(
                f"{self.base_url}/",
                url.lstrip("/"),
            )
        )
        allow = allow_status or set()

        response = self.session.request(
            method,
            request_url,
            headers=headers,
            params=params,
            data=data,
            json=json,
            timeout=_TIMEOUT_SECONDS,
            verify=True,
        )

        if response.status_code == 401:
            raise XWikiAuthenticationError(
                f"Authentication failed calling {request_url}"
            )

        if response.status_code in allow or 200 <= response.status_code < 300:
            return response

        # Let retry_builder handle retryable errors
        raise XWikiUnexpectedResponse(
            f"Unexpected status code {response.status_code} for {method} {request_url}. "
            f"Body: {response.text[:500]}"
        )

    def _load_page_data(self, document: dict[str, Any]) -> XWikiPage | None:

        # Language-based filtering (dedupe translations): only keep language None/empty
        language = document.get("language")
        if language not in (None, "", "null"):
            return None

        # Extract the REST page href
        links = document.get("links") or document.get("link") or {}
        # Some instances return {"links": {"link": [ ... ]}}
        if isinstance(links, dict) and "link" in links:
            links = links.get("link")
        if isinstance(links, dict):
            links = [links]
        if not isinstance(links, list):
            links = []

        page_rest_url: str | None = None
        for link in links:
            if not isinstance(link, dict):
                continue
            rel = link.get("rel")
            href = link.get("href") or ""
            if rel == "http://www.xwiki.org/rel/page" and "/translations/" not in str(
                    href
            ):
                page_rest_url = str(href)
                break

        if not page_rest_url:
            return None

        pageData = self._fetch_page_data(page_rest_url)
        if not pageData:
            return None

        page_id = pageData.get("id")
        page_url = pageData.get("xwikiAbsoluteUrl")
        modified_ms = pageData.get("modified")
        full_name = pageData.get("fullName")

        content = self._get_page_html(page_url=page_url, full_name=full_name)
        attachments = self._list_attachments(pageData.get("attachments"))
        return XWikiPage(
            id=page_id,
            full_name=full_name,
            page_url=page_url,
            modified_ms=modified_ms,
            content=content,
            attachments=attachments,
        )

    @retry_builder()
    def _fetch_page_data(self, page_url):
        if not page_url:
            return {}
        response = self._request(
            "GET",
            page_url,
            headers={"Accept": "application/json"},
            params={"attachments": "true"},
        )
        try:
            data = response.json() or {}
        except Exception:
            logger.exception("Failed to load page %s", page_url)
            data = {}
        return data

    def _esc(self, val: str) -> str:
        # minimal escaping for quotes and backslashes in a term
        return val.replace("\\", "\\\\").replace('"', '\\"')

    def _extract_space_from_page_reference(self, page_ref: str) -> str:
        """Extract space name from page reference (e.g., 'Sandbox.WebHome' -> 'Sandbox')"""
        if page_ref.endswith(".WebHome"):
            return page_ref[:-8]  # Remove ".WebHome"
        if "." in page_ref:
            # Assume it's a space name like "Help.Macros"
            return page_ref
        # No dot: simple space name like "Sandbox"
        return page_ref

    def _build_space_filter(self, space: str, recursive: bool) -> str:
        """Build SOLR space filter query"""
        if recursive:
            if " " in space:
                # Multi-word space: use required terms with wildcard
                words = space.split()
                required_terms = " ".join(f"+{word}" for word in words[:-1])
                last_word_wildcard = f"+{words[-1]}*"
                return f'space:({required_terms} {last_word_wildcard})'
            else:
                # Single-word space: use parentheses for cleaner syntax
                return f'space:({space}*)'
        else:
            # Exact match (needs quoting for multi-word spaces)
            return f'space:"{self._esc(space)}"'

    def _build_solr_query(
            self,
            root_page: str | None,
            tag: str | None,
            index_recursively: bool,
            since_ms: int | None,
    ) -> tuple[str, dict[str, str]]:
        """
        Build SOLR query and wiki parameter.

        Returns: (query_string, extra_params)

        When root_page contains a wiki prefix (e.g., "xwiki:Main.WebHome"),
        the wiki name is extracted and returned in extra_params to filter
        results to that specific wiki only.
        """
        queryTerms = ['type:("DOCUMENT")', "hidden:false"]
        extra_params = {}

        # Apply mutually exclusive filters
        if root_page:
            # Parse root_page to extract wiki prefix and page reference
            # Examples:
            #   "xwiki:Sandbox.WebHome" -> wiki="xwiki", page_ref="Sandbox.WebHome"
            #   "Sandbox.WebHome" -> wiki=None, page_ref="Sandbox.WebHome"
            wiki_prefix, separator, page_ref = root_page.partition(":")

            if separator:
                # Wiki prefix found: store it to filter by specific wiki
                extra_params["wiki"] = wiki_prefix
                space = self._extract_space_from_page_reference(page_ref)
            else:
                # No wiki prefix: use the entire root_page as page reference
                space = self._extract_space_from_page_reference(root_page)

            queryTerms.append(self._build_space_filter(space, index_recursively))

        elif tag:
            queryTerms.append(f'(property.XWiki.TagClass.tags:"{self._esc(tag)}")')

        # Time filter (works with any configuration)
        if since_ms is not None:
            queryTerms.append(_build_modified_filter(since_ms))

        query = " AND ".join([f"({t})" for t in queryTerms if t])
        return query, extra_params


    def query_pages(
            self,
            wiki,
            root_page: str | None,
            tag: str | None,
            index_recursively: bool,
            since_ms: int | None,
            start: int = 0,
            number: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[XWikiPage], int]:
        """
        Query pages from XWiki using SOLR search.

        Filter configuration (mutually exclusive):
        - wiki: Filter by wiki name (main or subwiki)
        - root_page: Filter by space hierarchy (e.g., "Sandbox.WebHome", "xwiki:Help.Macros")
        - tag: Filter by XWiki tag

        Note on translation filtering:
        Fetches all language versions; translations are filtered in _load_page_data().
        """
        # Build SOLR query and extract wiki parameter from root_page if present
        query, extra_params = self._build_solr_query(root_page, tag, index_recursively, since_ms)

        # Build request parameters
        parameters = {
            "type": "solr",
            "start": start,
            "number": number,
            "distinct": "true",
            "orderField": "date",
            "order": "asc",
            "q": query
        }

        # Apply wiki filter: prefer explicit wiki parameter, fallback to extracted from root_page
        wiki_to_use = wiki or extra_params.get("wiki")
        if wiki_to_use:
            parameters["wikis"] = wiki_to_use

        response = self._request(
            "GET",
            "rest/wikis/query",
            headers={"Accept": "application/json"},
            params=parameters,
        )
        data = response.json() or {}

        sr = data.get("searchResults") if isinstance(data, dict) else None
        if isinstance(sr, list):
            data_list = sr
        else:
            data_list = []

        refs: list[XWikiPage] = []
        for document in data_list:
            ref = self._load_page_data(document)
            if not ref:
                continue
            refs.append(ref)

        raw_count = len(data_list)
        return refs, raw_count

    def _list_attachments(self, data: dict[str, Any]) -> list[XWikiAttachment]:
        # Use the REST page URL directly to list attachments
        if (
                not data
                or not isinstance(data, dict)
                or "attachments" not in data
                or not isinstance(data.get("attachments"), list)
        ):
            return []

        attachments_data = data.get("attachments")
        attachments: list[XWikiAttachment] = []
        for entry in attachments_data:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not name:
                continue
            url = entry.get("xwikiAbsoluteUrl") or entry.get("download")
            attachments.append(
                XWikiAttachment(
                    name=str(name),
                    size=entry.get("size"),
                    mime_type=entry.get("mimeType"),
                    download_path=url,
                    version=entry.get("version"),
                    digest=entry.get("digest"),
                )
            )
        return attachments

    @retry_builder()
    def download_attachment(self, attachment: XWikiAttachment) -> bytes:
        response = self._request("GET", attachment.download_path)
        return response.content or b""

    def attachment_download_url(self, attachment: XWikiAttachment) -> str:
        if attachment.download_path.startswith("http"):
            return attachment.download_path
        return urljoin(f"{self.base_url}/", attachment.download_path.lstrip("/"))

    def resolve_page_content_url(self, view_url: str) -> str:
        """
        Convert an XWiki 'view' URL to the matching 'get' URL (content-only).
        """
        parts = urlsplit(view_url)
        # Replace /bin/view/ or /wiki/.../view/ with /get/
        new_path = parts.path.replace("/bin/view/", "/bin/get/")
        new_path = new_path.replace("/wiki/", "/wiki/", 1).replace("/view/", "/get/", 1) if "/wiki/" in new_path else new_path

        query = "xpage=plain&viewer=content&outputSyntax=html"
        return f"{parts.scheme}://{parts.netloc}{new_path}?{query}"

        return f"{parts.scheme}://{parts.netloc}{new_path}?{query}"

    @retry_builder()
    def _get_page_html(self, page_url: str, full_name: str) -> str:
        """
        Fetch content-only HTML for a page URL and convert it to clean text.
        """
        get_url = self.resolve_page_content_url(page_url)
        response = self._request(
            "GET",
            get_url,  # already fully-qualified; don't urljoin again
            headers={"Accept": "text/html"},
        )
        soup = bs4.BeautifulSoup(response.text or "", "html.parser")
        return format_document_soup(soup)
