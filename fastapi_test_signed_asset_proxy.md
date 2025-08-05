# 🧪 FastAPI Test for Signed Asset Proxy (Cloudflare Worker)

## 🎯 Goal
Create a FastAPI test using `httpx.AsyncClient` and `pytest` that verifies the proxy is correctly serving a signed DigitalOcean Spaces asset through `https://signed.mfcoapi.com`.

## 📄 Asset Info

- **Original (signed) URL**:  
  `https://mfcoapi.nyc3.digitaloceanspaces.com/appomattox/repositories/APXV1-ALICE/apx-town-of-appomattox/toa-council-members.pdf`

- **Proxied URL to test**:  
  `https://signed.mfcoapi.com/appomattox/repositories/APXV1-ALICE/apx-town-of-appomattox/toa-council-members.pdf`

## ✅ Test Requirements

- Use `pytest` with `httpx.AsyncClient`
- Fetch the file from the proxied URL
- Assert:
  - `status_code == 200`
  - `Content-Type == application/pdf`
  - Body starts with `%PDF` (common PDF magic bytes)

## 🧪 Sample Test Function

```python
import pytest
import httpx

@pytest.mark.asyncio
async def test_signed_asset_proxy_pdf():
    url = "https://signed.mfcoapi.com/appomattox/repositories/APXV1-ALICE/apx-town-of-appomattox/toa-council-members.pdf"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/pdf"
    assert response.content.startswith(b"%PDF")
```
