from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Response,
    status,
)

from diracx.core.config import Config, get_config
from diracx.core.properties import SecurityProperty

from .utils import has_properties

router = APIRouter(
    tags=["config"],
    dependencies=[has_properties(SecurityProperty.NORMAL_USER)],
)


LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


@router.get("/{vo}")
async def serve_config(
    vo: str,
    config: Annotated[Config, Depends(get_config)],
    response: Response,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    """ "
    Get the latest view of the config.


    If If-None-Match header is given and matches the latest ETag, return 304

    If If-Modified-Since is given and is newer than latest,
        return 304: this is to avoid flip/flopping
    """
    headers = {
        "ETag": config._hexsha,
        "Last-Modified": config._modified.strftime(LAST_MODIFIED_FORMAT),
    }

    if if_none_match == config._hexsha:
        raise HTTPException(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)

    # This is to prevent flip/flopping in case
    # a server gets out of sync with disk
    if if_modified_since:
        try:
            not_before = datetime.strptime(
                if_modified_since, LAST_MODIFIED_FORMAT
            ).astimezone(timezone.utc)
        except ValueError:
            pass
        else:
            if not_before > config._modified:
                raise HTTPException(
                    status_code=status.HTTP_304_NOT_MODIFIED, headers=headers
                )

    response.headers.update(headers)

    return config
