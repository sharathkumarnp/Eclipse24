import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import httpx

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event

TOKENIZER_URL = settings.UMBRELLA_AUTH_URL or "https://tokenizer.umbrellacost.io/prod/credentials"
BASE_URL = settings.UMBRELLA_BASE_URL or "https://api.umbrellacost.io"

UMBRELLA_VERIFY_SSL = False


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


@dataclass
class _Creds:
    bearer: str
    base_apikey: str
    expires_at: float


class UmbrellaCostClient:
    _creds: Optional[_Creds] = None
    _acct_cache: Dict[str, Tuple[str, str]] = {}
    _acct_cache_expires_at: float = 0

    def __init__(self):
        self.user = settings.UMBRELLA_USER
        self.password = settings.UMBRELLA_PASSWORD

    def infer_cloud_from_region(self, region: str) -> str:
        r = _norm(region)
        if "gcprod" in r or "gcp" in r:
            return "gcp"
        if "azprod" in r or "azure" in r:
            return "azure"
        if "prod" in r or "aws" in r:
            return "aws"
        return "aws"

    def _account_name_for_cloud(self, cloud: str) -> str:
        rules = settings.load_rules() or {}
        cfg = ((rules.get("config") or {}).get("umbrella_accounts") or {}).get(cloud) or {}
        name = cfg.get("accountName")
        if not name:
            raise RuntimeError(f"umbrella_accounts.{cloud}.accountName missing in rules.yaml")
        return name

    async def _get_creds(self) -> _Creds:
        now = time.time()
        if self._creds and now < self._creds.expires_at:
            return self._creds

        if not (self.user and self.password):
            raise RuntimeError("UmbrellaCost credentials missing")

        async with httpx.AsyncClient(timeout=15, verify=UMBRELLA_VERIFY_SSL) as client:
            r = await client.post(
                TOKENIZER_URL,
                json={"username": self.user, "password": self.password},
                headers={"Content-Type": "application/json"},
            )
            r.raise_for_status()
            data = r.json() or {}

        bearer = data.get("Authorization")
        base_apikey = data.get("apikey")
        if not bearer or not base_apikey:
            raise RuntimeError(f"Bad tokenizer response: {data}")

        self._creds = _Creds(
            bearer=bearer,
            base_apikey=base_apikey,
            expires_at=now + 50 * 60,
        )
        log_event("umbrella", "tokenizer_ok", {"ttl": 50 * 60})
        return self._creds

    async def _refresh_users_cache(self):
        now = time.time()
        if self._acct_cache and now < self._acct_cache_expires_at:
            return

        creds = await self._get_creds()

        async with httpx.AsyncClient(timeout=20, verify=UMBRELLA_VERIFY_SSL) as client:
            r = await client.get(
                f"{BASE_URL}/api/v1/users",
                headers={
                    "apikey": creds.base_apikey,
                    "Authorization": creds.bearer,
                },
            )
            r.raise_for_status()
            data: Any = r.json() or {}

        accounts: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            accounts = data.get("accounts") or []
        elif isinstance(data, list):
            accounts = data
        else:
            log_event(
                "umbrella",
                "users_unexpected_shape",
                {"type": str(type(data))},
            )

        cache: Dict[str, Tuple[str, str]] = {}
        if isinstance(accounts, list):
            for acc in accounts:
                if not isinstance(acc, dict):
                    continue
                name = (acc.get("accountName") or "").strip()
                account_key = acc.get("accountKey")
                division_id = acc.get("divisionId")
                if not name or account_key is None or division_id is None:
                    continue
                cache[name] = (str(account_key), str(division_id))

        self._acct_cache = cache
        self._acct_cache_expires_at = now + 6 * 60 * 60
        log_event(
            "umbrella",
            "users_cache_ok",
            {"count": len(cache), "names": list(cache.keys())},
        )

    async def get_cost_for_namespace(
            self,
            namespace: str,
            cloud: Optional[str] = None,
            region: Optional[str] = None,
            start_date: str = "",
            end_date: str = "",
            granularity: str = "daily",
            group_by: str = "namespace",
    ) -> Dict[str, Any]:
        if not cloud:
            cloud = self.infer_cloud_from_region(region or "")
        cloud = cloud.lower()

        creds = await self._get_creds()
        await self._refresh_users_cache()

        account_name = self._account_name_for_cloud(cloud)
        acct = self._acct_cache.get(account_name)
        if not acct:
            log_event(
                "umbrella",
                "account_not_found",
                {
                    "cloud": cloud,
                    "wanted": account_name,
                    "available": list(self._acct_cache.keys()),
                },
            )
            raise RuntimeError(f"accountName '{account_name}' missing in Umbrella /users")

        account_key, division_id = acct

        raw_base = creds.base_apikey or ""
        if raw_base.endswith(":-1"):
            base_apikey = raw_base[:-3]
        else:
            base_apikey = raw_base

        hdr_key = f"{base_apikey}:{account_key}:{division_id}"

        url = f"{BASE_URL}/api/v1/kubernetes/cost-and-usage"

        params: List[Tuple[str, str]] = [
            ("groupBy", "namespace"),
            ("groupBy", "usagedate"),
            ("startDate", start_date),
            ("endDate", end_date),
            ("periodGranLevel", "day"),
            ("costUsageType", "compute"),
            ("costUsageType", "network"),
            ("costUsageType", "dataTransfer"),
            ("costUsageType", "storage"),
            (f"filters[{group_by}]", namespace),
        ]

        if cloud == "gcp":
            params.append(("k8SGranularity", "PODS"))

        async with httpx.AsyncClient(timeout=25, verify=UMBRELLA_VERIFY_SSL) as client:
            r = await client.get(
                url,
                params=params,
                headers={
                    "apikey": hdr_key,
                    "Authorization": creds.bearer,
                    "commonParams": '{"isPpApplied":false}',
                },
            )
            if r.status_code in (401, 403):
                self._creds = None
                creds = await self._get_creds()

                raw_base = creds.base_apikey or ""
                if raw_base.endswith(":-1"):
                    base_apikey = raw_base[:-3]
                else:
                    base_apikey = raw_base

                hdr_key = f"{base_apikey}:{account_key}:{division_id}"

                r = await client.get(
                    url,
                    params=params,
                    headers={
                        "apikey": hdr_key,
                        "Authorization": creds.bearer,
                        "commonParams": '{"isPpApplied":false}',
                    },
                )
            r.raise_for_status()
            return r.json() or {}

    async def get_k8s_cost_for_namespace(
            self,
            namespace: str,
            cloud: Optional[str] = None,
            region: Optional[str] = None,
            start_date: str = "",
            end_date: str = "",
            granularity: str = "daily",
    ) -> Dict[str, Any]:
        return await self.get_cost_for_namespace(
            namespace=namespace,
            cloud=cloud,
            region=region,
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
            group_by="namespace",
        )