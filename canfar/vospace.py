"""CANFAR VOSpace Management."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import vos

from canfar import get_logger
from canfar.client import HTTPClient

if TYPE_CHECKING:
    pass

log = get_logger(__name__)

# Default VOSpace webservice host for CANFAR CLI
DEFAULT_VOSPACE_HOST = "spsrc27.iaa.csic.es"

# Header used by vos library for delegation token auth
HEADER_DELEG_TOKEN = "X-CADC-DelegationToken"


class VOSpaceClient(HTTPClient):
    """VOSpace client that inherits authentication from HTTPClient.

    This client automatically uses the active CANFAR authentication context
    to create an authenticated vos.Client instance.

    The default VOSpace host is spsrc27.iaa.csic.es. This can be overridden
    by setting the VOSPACE_WEBSERVICE environment variable.

    Note:
        This client uses standard OAuth2 Bearer token authentication
        (Authorization: Bearer header) instead of the X-CADC-DelegationToken
        header for broader compatibility with VOSpace services.

    Examples:
        >>> from canfar.vospace import VOSpaceClient
        >>> vospace = VOSpaceClient()
        >>> vospace.vos_client.listdir("vos:")
    """

    def __init__(self, **kwargs):
        """Initialize VOSpaceClient with HTTPClient authentication."""
        super().__init__(**kwargs)
        self._vos_client = None
        self._token = None

    @property
    def vos_client(self) -> vos.Client:
        """Get or create authenticated vos.Client instance.

        Returns:
            vos.Client: Authenticated VOSpace client using the active context's token.
        """
        if self._vos_client is None:
            # Set default VOSpace host if not already configured
            if not os.getenv("VOSPACE_WEBSERVICE") and not os.getenv("LOCAL_VOSPACE_WEBSERVICE"):
                os.environ["VOSPACE_WEBSERVICE"] = DEFAULT_VOSPACE_HOST
                log.debug(f"Using default VOSpace host: {DEFAULT_VOSPACE_HOST}")

            # Extract token from active authentication context
            ctx = self.config.context

            # Get access token based on auth mode
            if hasattr(ctx, 'token') and ctx.token:
                self._token = ctx.token.access
            else:
                # Fallback for X509 or other auth modes
                self._token = None

            # Create the vos.Client with token
            self._vos_client = vos.Client(vospace_token=self._token)

            # Patch to use Bearer auth for broader compatibility
            # Some VOSpace servers (e.g., canfar.srcnet.skao.int) only accept
            # Authorization: Bearer, while others (e.g., sweSRC) accept both
            if self._token:
                self._patch_bearer_auth()

        return self._vos_client

    def _patch_bearer_auth(self) -> None:
        """Patch the vos.Client to use Bearer token authentication.

        This monkey-patches the get_endpoints method to add Bearer auth
        headers to sessions as endpoints are created.
        """
        token = self._token
        original_get_endpoints = self._vos_client.get_endpoints

        def patched_get_endpoints(uri):
            """Wrapper that adds Bearer auth after endpoint creation."""
            endpoint = original_get_endpoints(uri)

            # Modify the session headers for this endpoint
            try:
                ws_client = endpoint.conn.ws_client

                # Clear X-CADC-DelegationToken from session_headers
                # (this prevents it from being re-added on each session access)
                if ws_client.session_headers and HEADER_DELEG_TOKEN in ws_client.session_headers:
                    del ws_client.session_headers[HEADER_DELEG_TOKEN]

                # Get the session and modify its headers
                session = ws_client._get_session()

                # Remove X-CADC-DelegationToken if present
                if HEADER_DELEG_TOKEN in session.headers:
                    del session.headers[HEADER_DELEG_TOKEN]

                # Add Bearer auth if not already present
                if "Authorization" not in session.headers:
                    session.headers["Authorization"] = f"Bearer {token}"
                    log.debug(f"Added Bearer auth for {uri}")

            except AttributeError as e:
                log.warning(f"Could not modify auth headers for {uri}: {e}")

            return endpoint

        # Replace the method
        self._vos_client.get_endpoints = patched_get_endpoints
        log.debug("Patched vos.Client to use Bearer authentication")
