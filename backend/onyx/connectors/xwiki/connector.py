from __future__ import annotations

import copy
from typing import Any

from typing_extensions import override
from datetime import datetime, timezone

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.exceptions import UnexpectedValidationError
from onyx.connectors.interfaces import CheckpointedConnector
from onyx.connectors.interfaces import CheckpointOutput
from onyx.connectors.interfaces import ConnectorCheckpoint
from onyx.connectors.interfaces import CredentialsProviderInterface
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.models import Document
from onyx.connectors.models import ImageSection
from onyx.connectors.models import TextSection
from onyx.connectors.xwiki.onyx_xwiki import XWikiClient
from onyx.connectors.xwiki.onyx_xwiki import XWikiPage
from onyx.connectors.xwiki.utils import AttachmentProcessingResult
from onyx.connectors.xwiki.utils import process_attachment
from onyx.utils.logger import setup_logger

logger = setup_logger()


class XWikiCheckpoint(ConnectorCheckpoint):
    since_ms: int | None = None  # inclusive modified threshold (ms)
    offset: int = 0


class XWikiConnector(CheckpointedConnector[XWikiCheckpoint]):
    def __init__(
        self,
        base_url: str,
        wiki: str,
        root_page: str | None = None,
        index_recursively: bool = True,
        tag: str | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self._base_url = base_url
        self._wiki = wiki
        self._root_page = root_page
        self._index_recursively = index_recursively
        self._tag = tag
        self._batch_size = batch_size
        self._client: XWikiClient | None = None
        self._credentials_provider: CredentialsProviderInterface | None = None
        self._allow_images = False

    def set_allow_images(self, allow_images: bool) -> None:
        """Enable or disable image attachment downloading/processing."""
        self._allow_images = allow_images

    @override
    def load_credentials(
        self, credentials: dict[str, Any]
    ) -> dict[str, Any] | None:  # pragma: no cover

        username = credentials["xwiki_username"]
        password = credentials["xwiki_password"]
        self._client = XWikiClient(
            base_url=self._base_url,
            username=username,
            password=password,
        )

    @override
    def build_dummy_checkpoint(self) -> XWikiCheckpoint:
        return XWikiCheckpoint(has_more=True)

    @override
    def validate_checkpoint_json(self, checkpoint_json: str) -> XWikiCheckpoint:
        return XWikiCheckpoint.model_validate_json(checkpoint_json)

    def validate_connector_settings(self) -> None:
        if self._client is None:
            raise ConnectorMissingCredentialError("xwiki")

        try:
            self._client.query_pages(
                wiki=self._wiki,
                root_page=self._root_page,
                tag=self._tag,
                index_recursively=self._index_recursively,
                since_ms=None,
                start=0,
                number=1,
            )
        except Exception as exc:  # pragma: no cover - external failure surface
            raise UnexpectedValidationError(
                f"Unexpected error while validating XWiki settings: {exc}"
            )

    @override
    def load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: XWikiCheckpoint,
    ) -> CheckpointOutput[XWikiCheckpoint]:
        if self._client is None:
            raise ConnectorMissingCredentialError("xwiki")

        cp = copy.deepcopy(checkpoint)

        # Initialize since_ms from start timestamp on first run
        if cp.since_ms is None and start:
            cp.since_ms = int(start * 1000)  # Convert to milliseconds

        pages, raw_count = self._client.query_pages(
            wiki=self._wiki,
            root_page=self._root_page,
            tag=self._tag,
            index_recursively=self._index_recursively,
            since_ms=cp.since_ms,
            start=cp.offset,
            number=self._batch_size,
        )

        # Track latest modified time for next incremental sync
        max_modified = cp.since_ms or 0
        for page in pages:
            if page.modified_ms and page.modified_ms > max_modified:
                max_modified = page.modified_ms

            try:
                document = process_xwiki_page(
                    client=self._client, page=page, allow_images=self._allow_images
                )
                yield document
            except Exception as exc:  # pragma: no cover - network/parse errors
                logger.warning(
                    "Failed to fetch XWiki page %s: %s",
                    page.full_name,
                    exc,
                )
                continue

        cp.offset += raw_count
        cp.has_more = raw_count >= self._batch_size

        # Reset offset and update since_ms when batch is complete
        if not cp.has_more:
            cp.offset = 0
            if max_modified > 0:
                cp.since_ms = max_modified

        return cp


def process_xwiki_page(
    client: XWikiClient, page: XWikiPage, allow_images: bool = False
) -> Document:
    sections: list[TextSection | ImageSection] = [
        TextSection(text=page.content, link=page.page_url)
    ]

    try:
        for attachment in page.attachments:
            result: AttachmentProcessingResult = process_attachment(
                client=client,
                page_ref=page,
                attachment=attachment,
                allow_images=allow_images,
            )
            if result.text:
                # Link to the download URL
                download_url = client.attachment_download_url(attachment)
                sections.append(TextSection(text=result.text, link=download_url))
            if result.image_section:
                sections.append(result.image_section)
    except Exception as exc:  # pragma: no cover - optional features
        logger.warning("Failed to append attachment sections: %s", exc)

    document = Document(
        id=page.id,
        source=DocumentSource.XWIKI,
        title=page.full_name,
        semantic_identifier=page.full_name,
        sections=sections,
        metadata={
            "name": page.full_name,
        },
        doc_updated_at=datetime.fromtimestamp(page.modified_ms / 1000, tz=timezone.utc),
    )
    return document


if __name__ == "__main__":
    import os

    from onyx.utils.variable_functionality import global_version
    from tests.daily.connectors.utils import load_all_docs_from_checkpoint_connector

    global_version.set_ee()

    base_url = os.environ["XWIKI_BASE_URL"]
    wiki = os.environ.get("XWIKI_WIKI")
    root_page = os.environ.get("XWIKI_ROOT_PAGE")
    tag = os.environ.get("XWIKI_TAG")
    index_recursively = os.environ.get("XWIKI_INDEX_RECURSIVELY", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    allow_images = os.environ.get("XWIKI_ALLOW_IMAGES", "false").lower() in (
        "1",
        "true",
        "yes",
    )

    connector = XWikiConnector(
        base_url=base_url,
        wiki=wiki,
        root_page=root_page,
        index_recursively=index_recursively,
        tag=tag,
    )

    connector.set_allow_images(allow_images=True)

    connector.load_credentials(
        {
            "xwiki_username": os.environ["XWIKI_USERNAME"],
            "xwiki_password": os.environ["XWIKI_PASSWORD"],
        }
    )

    start = 0
    end = int(datetime.now(tz=timezone.utc).timestamp())

    docs = load_all_docs_from_checkpoint_connector(
        connector=connector,
        start=start,
        end=end,
    )

    # Print results
    doc_count = 0
    for doc in docs:
        print(doc)
        doc_count += 1

    print(f"Documents loaded: {doc_count}")
    print(f"Documents {len(list(docs))}")
