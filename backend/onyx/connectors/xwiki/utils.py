from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from onyx.configs.app_configs import (
    CONFLUENCE_CONNECTOR_ATTACHMENT_CHAR_COUNT_THRESHOLD,
)
from onyx.configs.app_configs import CONFLUENCE_CONNECTOR_ATTACHMENT_SIZE_THRESHOLD
from onyx.configs.constants import FileOrigin
from onyx.connectors.xwiki.onyx_xwiki import XWikiClient
from onyx.connectors.xwiki.onyx_xwiki import XWikiPage
from onyx.file_processing.extract_file_text import extract_file_text
from onyx.file_processing.extract_file_text import is_accepted_file_ext
from onyx.file_processing.extract_file_text import OnyxExtensionType
from onyx.file_processing.file_validation import is_valid_image_type
from onyx.file_processing.image_utils import store_image_and_create_section
from onyx.utils.logger import setup_logger

logger = setup_logger()

# NOTE:
# XWiki currently reuses the same thresholds as the Confluence connector.
# If XWiki requires different limits in the future, introduce dedicated
# XWIKI_* settings in app_configs and swap these aliases accordingly.
XWIKI_ATTACHMENT_SIZE_THRESHOLD = CONFLUENCE_CONNECTOR_ATTACHMENT_SIZE_THRESHOLD
XWIKI_ATTACHMENT_CHAR_COUNT_THRESHOLD = (
    CONFLUENCE_CONNECTOR_ATTACHMENT_CHAR_COUNT_THRESHOLD
)


class AttachmentProcessingResult(BaseModel):
    """
    Container for the outcome of processing a single XWiki attachment.

    - text: extracted text for document-like attachments (None for images)
    - image_section: ImageSection created and stored by the FileStore for image attachments
    - error: a human readable reason if the processing failed or was skipped
    """

    text: str | None = None
    image_section: Any | None = (
        None  # typed as Any to avoid circular import on ImageSection
    )
    error: str | None = None


def validate_attachment_filetype(attachment: Any) -> bool:
    """
    Validate whether an attachment is acceptable for processing.

    Rules mirror Confluence connector behavior:
      - Images: allowed only if their media type is one of the supported image types
      - Non-images: allowed only if the file extension matches accepted plain/document types
    """
    media_type = (getattr(attachment, "mime_type", None) or "").lower()
    if media_type.startswith("image/"):
        return is_valid_image_type(media_type)

    # For non-image files, ensure we support the extension
    suffix = Path(getattr(attachment, "name", "")).suffix
    if not suffix:
        return False

    return is_accepted_file_ext(
        suffix, OnyxExtensionType.Plain | OnyxExtensionType.Document
    )


def _make_storage_file_id(page_ref: XWikiPage, attachment: Any) -> str:
    """
    Build a deterministic FileStore key for the image attachment.

    We sanitize the page name and the attachment name to avoid problematic
    characters. This mirrors the deterministic approach used by Confluence
    (it uses the attachment ID); XWiki does not expose a globally unique
    numeric ID in the same way, so we combine page and file names.
    """
    safe_page = page_ref.full_name.replace(":", "_").replace("/", "_")
    safe_name = getattr(attachment, "name", "").replace(":", "_").replace("/", "_")
    return f"xwiki-{safe_page}-{safe_name}"


def process_attachment(
    *,
    client: XWikiClient,
    page_ref: XWikiPage,
    attachment: Any,
    allow_images: bool,
) -> AttachmentProcessingResult:
    """
    Process a single XWiki attachment.

    - If the attachment is an image and image downloading is allowed, store it
      in FileStore and return the created ImageSection.
    - If the attachment is a document, download it and extract text, while
      enforcing size and character count thresholds.

    Returns:
        AttachmentProcessingResult: a structured result with either text or
        an image_section, or an error message if the attachment was skipped.
    """
    media_type = getattr(attachment, "mime_type", None) or ""

    # Validate supported file types early
    if not validate_attachment_filetype(attachment):
        return AttachmentProcessingResult(
            error=f"Unsupported file type: {media_type or getattr(attachment, 'name', '')}",
        )

    download_url = client.attachment_download_url(attachment)
    attachment_size = getattr(attachment, "size", None) or 0

    # Enforce size limits only for non-image attachments (same as Confluence)
    if media_type.startswith("image/"):
        if not allow_images:
            return AttachmentProcessingResult(error="Image downloading is not enabled")
    else:
        if attachment_size and attachment_size > XWIKI_ATTACHMENT_SIZE_THRESHOLD:
            logger.warning(
                "Skipping %s due to attachment size (size=%s threshold=%s)",
                download_url,
                attachment_size,
                XWIKI_ATTACHMENT_SIZE_THRESHOLD,
            )
            return AttachmentProcessingResult(error="Attachment exceeds size limit")

    # Download raw bytes of the attachment
    try:
        raw_bytes = client.download_attachment(attachment)
    except Exception as exc:  # pragma: no cover - network/errors surface
        logger.warning(
            "Failed to download attachment %s: %s",
            getattr(attachment, "name", ""),
            exc,
        )
        return AttachmentProcessingResult(error=str(exc))

    if not raw_bytes:
        return AttachmentProcessingResult(error="Attachment content empty")

    # Handle images by storing and creating an ImageSection, no OCR/summary here
    if media_type.startswith("image/"):
        try:
            section, _ = store_image_and_create_section(
                image_data=raw_bytes,
                file_id=_make_storage_file_id(page_ref, attachment),
                display_name=getattr(attachment, "name", ""),
                link=download_url,
                media_type=media_type or "application/octet-stream",
                file_origin=FileOrigin.CONNECTOR,
            )
            return AttachmentProcessingResult(image_section=section)
        except Exception as exc:  # pragma: no cover - storage backend
            logger.warning(
                "Image storage failed for %s: %s",
                getattr(attachment, "name", ""),
                exc,
            )
            return AttachmentProcessingResult(error=str(exc))

    # For documents, extract text from the file bytes
    try:
        text = extract_file_text(
            file=BytesIO(raw_bytes),
            file_name=getattr(attachment, "name", ""),
        )
    except Exception as exc:
        logger.warning(
            "Failed to extract text for %s: %s",
            getattr(attachment, "name", ""),
            exc,
        )
        return AttachmentProcessingResult(error=str(exc))

    # Enforce character count threshold to avoid oversized content
    if len(text) > XWIKI_ATTACHMENT_CHAR_COUNT_THRESHOLD:
        logger.warning(
            "Skipping %s due to character count (chars=%s threshold=%s)",
            download_url,
            len(text),
            XWIKI_ATTACHMENT_CHAR_COUNT_THRESHOLD,
        )
        return AttachmentProcessingResult(error="Attachment text too long")

    return AttachmentProcessingResult(text=text or None)
