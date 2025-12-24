import os
from pathlib import Path
from typing import Optional, Dict, Any

import yaml
from dotenv import load_dotenv

load_dotenv()


def _proj_root() -> Path:
    # src/eclipse24/libs/core/config.py → project root
    return Path(__file__).resolve().parents[4]


def _load_yaml(p: Path) -> Dict[str, Any]:
    with p.open("r") as f:
        return yaml.safe_load(f) or {}


class Settings:
    """ECLIPSE configuration."""

    # --- Core ---
    RMQ_URL: Optional[str] = os.getenv("RMQ_URL")
    DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"

    # --- Database ---
    PERSIST_TO_DB: bool = os.getenv("PERSIST_TO_DB", "false").lower() == "true"
    PG_DSN: Optional[str] = os.getenv("PG_DSN")  # ✅ fixed — removed stray bracket

    # --- Slack ---
    SLACK_BOT_TOKEN: Optional[str] = os.getenv("SLACK_BOT_TOKEN")
    SLACK_SIGNING_SECRET: Optional[str] = os.getenv("SLACK_SIGNING_SECRET")
    SLACK_APPROVAL_CHANNEL: str = os.getenv("SLACK_APPROVAL_CHANNEL", "#safeguard-test-approvals")

    # --- Store API ---
    STORE_API_URL: str = os.getenv("STORE_API_URL", "https://storedashboard.jfrog.info")
    STORE_USER: Optional[str] = os.getenv("STORE_USER")
    STORE_PASSWORD: Optional[str] = os.getenv("STORE_PASSWORD")

    # --- Cloud Usage ---
    CLOUD_USAGE_BASE: str = os.getenv("CLOUD_USAGE_BASE", "https://cloud-usage.jfrog.info")
    CLOUD_USAGE_API_KEY: Optional[str] = os.getenv("CLOUD_USAGE_API_KEY")

    # --- Narcissus (fallbacks kept for poller if you decide to use it) ---
    NARCISSUS_TOKEN: Optional[str] = os.getenv("NARCISSUS_TOKEN")
    NARCISSUS_USERNAME: Optional[str] = os.getenv("NARCISSUS_USERNAME")
    NARCISSUS_PASSWORD: Optional[str] = os.getenv("NARCISSUS_PASSWORD")
    NARCISSUS_ENV: str = os.getenv("NARCISSUS_ENV", "production")

    # --- New Relic placeholders (for future) ---
    NEWRELIC_API_KEY: Optional[str] = os.getenv("NEWRELIC_API_KEY")
    NEWRELIC_ACCOUNT_ID: Optional[str] = os.getenv("NEWRELIC_ACCOUNT_ID")
    NEWRELIC_REGION: str = os.getenv("NEWRELIC_REGION", "US")

    # --- Kafka (MSK IAM ready) ---
    #   KAFKA_SECURITY_PROTOCOL: SASL_SSL
    #   KAFKA_SASL_MECHANISM: OAUTHBEARER
    #   KAFKA_REGION: ap-south-1
    KAFKA_BROKERS: str = os.getenv("KAFKA_BROKERS", "localhost:9092")  # --> AWS KAFKA BOOTSTRAP_SERVERS ENDPOINT
    KAFKA_SECURITY_PROTOCOL: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    KAFKA_SASL_MECHANISM: str = os.getenv("KAFKA_SASL_MECHANISM", "")
    KAFKA_SASL_USERNAME: Optional[str] = os.getenv("KAFKA_SASL_USERNAME")
    KAFKA_SASL_PASSWORD: Optional[str] = os.getenv("KAFKA_SASL_PASSWORD")
    KAFKA_GROUP_ID_PREFIX: str = os.getenv("KAFKA_GROUP_ID_PREFIX", "eclipse24")
    KAFKA_CLIENT_ID: str = os.getenv("KAFKA_CLIENT_ID", "eclipse24")
    KAFKA_REGION: str = os.getenv("KAFKA_REGION", os.getenv("AWS_REGION", "us-east-1"))
    KAFKA_AUTO_CREATE_TOPICS: bool = os.getenv("KAFKA_AUTO_CREATE_TOPICS", "false").lower() == "true"

    # --- Policy locations ---
    RULES_PATH: Path = Path(os.getenv("RULES_PATH") or _proj_root() / "src" / "eclipse24" / "configs" / "rules.yaml")
    QUEUES_PATH: Path = Path(
        os.getenv("QUEUES_PATH") or _proj_root() / "src" / "eclipse24" / "configs" / "queues.yaml")

    # --- Umbrellacost ---
    UMBRELLA_BASE_URL = os.getenv("UMBRELLA_BASE_URL", "https://api.umbrellacost.io")
    UMBRELLA_AUTH_URL = os.getenv("UMBRELLA_AUTH_URL", "")
    UMBRELLA_USER = os.getenv("UMBRELLA_USER", "")
    UMBRELLA_PASSWORD = os.getenv("UMBRELLA_PASSWORD", "")

    # --- Jira ---
    JIRA_USER: Optional[str] = os.getenv("ECLIPSE_JIRA_USER")
    JIRA_API_TOKEN: Optional[str] = os.getenv("ECLIPSE_JIRA_TOKEN")

    def load_rules(self) -> Dict[str, Any]:
        return _load_yaml(self.RULES_PATH)

    def load_queues(self) -> Dict[str, Any]:
        return _load_yaml(self.QUEUES_PATH)

    def queue_name(self, key: str) -> str:
        q = self.load_queues().get("queues", {}).get(key, {})
        name = q.get("name")
        if not name:
            raise KeyError(f"Queue '{key}' not defined in {self.QUEUES_PATH}")
        return name


settings = Settings()