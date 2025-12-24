import asyncio
from typing import Any, Dict, List, Optional

import httpx

from eclipse24.libs.core.config import settings
from eclipse24.libs.core.logging import log_event


class NewRelicClient:
    def __init__(self):
        self.api_key = settings.NEWRELIC_API_KEY
        self.region = (settings.NEWRELIC_REGION or "US").upper().strip()
        self.endpoint = (
            "https://api.eu.newrelic.com/graphql"
            if self.region == "EU"
            else "https://api.newrelic.com/graphql"
        )
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client:
            return self._client
        self._client = httpx.AsyncClient(
            timeout=30,
            headers={
                "Content-Type": "application/json",
                "API-Key": self.api_key or "",
            },
        )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    @staticmethod
    def _tags_to_map(tags: List[Dict[str, Any]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for t in tags or []:
            k = (t.get("key") or "").strip()
            vals = t.get("values") or []
            if not k:
                continue
            if len(vals) == 1:
                out[k] = vals[0]
            else:
                out[k] = vals
        return out

    def _build_query(self, cursor: Optional[str]) -> str:
        """
        Build the GraphQL query.

        Important: keep the entitySearch `query:` string on a single logical line,
        same as the old working implementation, to avoid any weird parsing issues
        in New Relic.
        """
        cursor_part = f', nextCursor: "{cursor}"' if cursor else ""

        search_query = (
            "domain = 'SYNTH' AND type = 'MONITOR' "
            "AND tags.eclipse_test = 'true' "
            "AND tags.artifactory_version IS NOT NULL "
            "AND tags.env = 'production'"
        )

        q = f"""
        {{
          actor {{
            entitySearch(query: "{search_query}") {{
              results{cursor_part} {{
                entities {{
                  name
                  tags {{
                    key
                    values
                  }}
                }}
                nextCursor
              }}
            }}
          }}
        }}
        """

        # Optional debug log of the exact search query we send
        log_event(
            "newrelic",
            "query_build",
            {"region": self.region, "cursor": bool(cursor), "search": search_query},
        )
        return q

    async def fetch_internal_servers(self, max_pages: int = 50) -> List[Dict[str, Any]]:

        if not self.api_key:
            log_event("newrelic", "skip", {"reason": "missing_api_key"})
            return []

        client = await self._get_client()
        results: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page = 0

        while page < max_pages:
            page += 1
            q = self._build_query(cursor)
            try:
                resp = await client.post(self.endpoint, json={"query": q})
                resp.raise_for_status()
                data = resp.json() or {}
            except Exception as e:
                log_event("newrelic", "query_error", {"page": page, "error": str(e)})
                break

            try:
                es = (
                    data.get("data", {})
                    .get("actor", {})
                    .get("entitySearch", {})
                    .get("results", {})
                )
                entities = es.get("entities") or []
                cursor = es.get("nextCursor")
            except Exception:
                log_event("newrelic", "parse_error", {"page": page})
                break

            if not entities:
                break

            for ent in entities:
                tags_map = self._tags_to_map(ent.get("tags") or [])
                nr_name = (ent.get("name") or "").strip()

                # Prefer namespace_identifier, then fall back through other candidates,
                # and finally the NR entity name as last resort.
                technical = (
                        tags_map.get("namespace_identifier")
                        or tags_map.get("namespace")
                        or tags_map.get("technicalServerName")
                        or tags_map.get("technical_servername")
                        or tags_map.get("server_id")
                        or tags_map.get("jpd_base_name")
                        or tags_map.get("serverName")
                        or nr_name
                )

                if not technical:
                    continue

                region = tags_map.get("region") or tags_map.get("cloud_region")

                results.append(
                    {
                        "technicalServerName": technical,
                        "region": region,
                        "nr_tags": tags_map,
                    }
                )

            if not cursor:
                break

        log_event("newrelic", "fetched", {"count": len(results), "pages": page})
        return results


if __name__ == "__main__":

    async def _t():
        c = NewRelicClient()
        rows = await c.fetch_internal_servers()
        print(len(rows))
        await c.close()

    asyncio.run(_t())