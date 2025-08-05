import pytest
import httpx


@pytest.mark.asyncio
async def test_signed_asset_proxy_pdf():
    """
    Test that the Cloudflare Worker proxy correctly serves a signed DigitalOcean Spaces asset
    through the signed.mfcoapi.com subdomain.
    """
    url = "https://signed.mfcoapi.com/appomattox/repositories/APXV1-ALICE/apx-town-of-appomattox/toa-council-members.pdf"

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            pytest.skip(f"Cannot connect to {url}. Error: {e}. Please ensure Cloudflare Worker is deployed and DNS is properly configured.")

    # Assert successful response
    assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    
    # Assert correct content type for PDF
    assert response.headers["content-type"] == "application/pdf", f"Expected content-type 'application/pdf', got '{response.headers.get('content-type')}'"
    
    # Assert PDF magic bytes at the start of the content
    assert response.content.startswith(b"%PDF"), "Response content does not start with PDF magic bytes '%PDF'"


@pytest.mark.asyncio
async def test_signed_asset_proxy_response_headers():
    """
    Additional test to verify that proper headers are returned by the proxy.
    """
    url = "https://signed.mfcoapi.com/appomattox/repositories/APXV1-ALICE/apx-town-of-appomattox/toa-council-members.pdf"

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            pytest.skip(f"Cannot connect to {url}. Error: {e}. Please ensure Cloudflare Worker is deployed and DNS is properly configured.")

    # Basic assertions
    assert response.status_code == 200
    
    # Verify we have content
    assert len(response.content) > 0, "Response content is empty"
    
    # Verify content-type header exists
    assert "content-type" in response.headers, "Missing content-type header"


@pytest.mark.asyncio
async def test_dns_resolution():
    """
    Test that the signed.mfcoapi.com domain resolves properly.
    This is a preliminary test to verify DNS configuration.
    """
    import socket
    
    try:
        # Try to resolve the domain
        result = socket.gethostbyname("signed.mfcoapi.com")
        print(f"DNS resolved signed.mfcoapi.com to: {result}")
        assert result is not None
    except socket.gaierror as e:
        pytest.fail(f"DNS resolution failed for signed.mfcoapi.com: {e}")