from stockops.config import eodhd_config  # , add additional providers here as needed

_CONFIG_MAP = {  # , add additional providers here as needed
    "EODHD": eodhd_config,
}


class ProviderConfig:
    def __init__(self, provider: str, exchange: str = "US"):
        self.provider = provider
        self.exchange = exchange
        self.cfg = self.set_cfg()
        self.tz_str = self.cfg.EXCHANGE_METADATA[self.exchange]["Timezone"]

    def set_cfg(self):
        try:
            return _CONFIG_MAP[self.provider]
        except KeyError as err:
            raise ValueError(f"Unsupported provider: {self.provider!r}. Supported: {list(_CONFIG_MAP)}") from err
