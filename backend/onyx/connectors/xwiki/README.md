# XWiki Connector

The XWiki connector indexes pages from a configured XWiki deployment and emits one Onyx `Document` per page. Each document contains the page body and any attachment content inlined as additional sections, mirroring the Confluence connector behaviour.

## Supported Configuration

| Field | Description |
| --- | --- |
| `base_url` | Base URL of your XWiki deployment (e.g. `https://wiki.example.com`). |
| `wiki` | Wiki identifier (e.g. `xwiki` for main wiki or `subwiki` for subwikis). |
| `root_page` | (Optional) Root page to index from. Can be a space homepage (e.g. `Main.WebHome`) or a nested page (e.g. `Docs.SubPage`). If omitted, all pages in the wiki are indexed. |
| `index_recursively` | (Optional) When `true` (default), indexes the root page and all its descendants. When `false`, indexes only the specified root page. |
| `tag` | (Optional) Filter pages by XWiki tag. Only pages with this tag will be indexed. |
| `batch_size` | (Optional) Number of pages to fetch per batch. Defaults to system INDEX_BATCH_SIZE. |
| `xwiki_username` | Username used for Basic authentication. |
| `xwiki_password` | Password or API token used for Basic authentication. |

## Behaviour Overview

* **Discovery via SOLR** – The connector queries the XWiki SOLR REST API for pages using configurable filters (wiki, root page, tags). Initial runs fetch all matching pages. Incremental runs request only pages whose `modified` timestamp is newer than the stored checkpoint. Responses are paginated and retried on 429 / 5xx responses.
* **Page content** – HTML content is fetched using XWiki's content-only view (`xpage=plain&viewer=content&outputSyntax=annotatedXHTML`). The HTML is then converted to clean text using BeautifulSoup. The resulting text forms the primary `TextSection` linked to the page view URL.
* **Inline attachments** – Attachments are enumerated via the REST endpoint for each page. Each attachment is downloaded with the same authenticated session used for API calls. Supported document types are passed through `extract_file_text` and appended as additional `TextSection`s. Images that pass validation are stored via `store_image_and_create_section` and appended as `ImageSection`s. Size and character-count thresholds match the Confluence connector. Unsupported or oversized attachments are skipped with warnings.
* **Incremental sync** – The checkpoint tracks the latest modified timestamp (`since_ms`) and pagination offset. When a batch is fully processed, the checkpoint updates `since_ms` to enable efficient incremental syncs.
* **Deletions** – The connector does not emit deletion documents. Pages removed between runs simply stop appearing in SOLR results, keeping behaviour aligned with Confluence.

## Security Notes

* All REST and download requests reuse a single authenticated `requests.Session` configured with HTTP Basic auth.
* SSL verification is enabled by default. Provide a custom certificate bundle at the requests layer if required.

## Example Usage

```python
from datetime import datetime, timezone
from onyx.connectors.xwiki.connector import XWikiConnector

connector = XWikiConnector(
    base_url="https://wiki.example.com",
    wiki="xwiki",
    root_page="Engineering.WebHome",
    index_recursively=True,
)
connector.load_credentials(
    {
        "xwiki_username": "connector-user",
        "xwiki_password": "secret-token",
    }
)
checkpoint = connector.build_dummy_checkpoint()

start = datetime(1970, 1, 1, tzinfo=timezone.utc).timestamp()
end = datetime.now(tz=timezone.utc).timestamp()

generator = connector.load_from_checkpoint(start, end, checkpoint)
try:
    while True:
        document = next(generator)
        # index document
except StopIteration as exc:
    new_checkpoint = exc.value
```
