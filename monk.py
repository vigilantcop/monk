import logging
import os
import random
import threading
import time

from jito_searcher_client import get_searcher_client
from jito_searcher_client.convert import versioned_tx_to_protobuf_packet
from jito_searcher_client.generated.bundle_pb2 import Bundle
from jito_searcher_client.generated.searcher_pb2 import (
    GetTipAccountsRequest,
    SendBundleRequest,
    SubscribeBundleResultsRequest,
)
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

logger = logging.getLogger(__name__)


class JitoSender:
    LOG_EVERY = 100

    def __init__(self, block_engine_url: str | None = None) -> None:
        self.block_engine_url = block_engine_url or os.environ.get(
            "JITO_BLOCK_ENGINE_URL", "mainnet.block-engine.jito.wtf"
        )
        self.auth_keypair = Keypair.from_base58_string(os.environ["JITO_AUTH_KEY"])
        self.client = get_searcher_client(self.block_engine_url, self.auth_keypair)
        self._lock = threading.Lock()
        self._stats = {"landed": 0, "rejected": 0, "dropped": 0}
        self._own: set[str] = set()
        self._sent = 0
        self._thread = threading.Thread(target=self._track_results, daemon=True)
        self._thread.start()

    def send(self, txs: list[VersionedTransaction], tip: int) -> str:
        accounts = self.client.GetTipAccounts(GetTipAccountsRequest()).accounts
        tip_account = Pubkey.from_string(random.choice(accounts))
        payer = self.auth_keypair.pubkey()
        blockhash = txs[-1].message.recent_blockhash
        tip_ix = transfer(
            TransferParams(from_pubkey=payer, to_pubkey=tip_account, lamports=tip)
        )
        tip_msg = Message.new_with_blockhash([tip_ix], payer, blockhash)
        tip_tx = VersionedTransaction(tip_msg, [self.auth_keypair])
        packets = [versioned_tx_to_protobuf_packet(tx) for tx in [*txs, tip_tx]]
        response = self.client.SendBundle(
            SendBundleRequest(bundle=Bundle(packets=packets))
        )
        with self._lock:
            self._own.add(response.uuid)
            self._sent += 1
        logger.info("bundle sent uuid=%s txs=%d tip=%d", response.uuid, len(packets), tip)
        return response.uuid

    def land_rate(self) -> tuple[int, int, float]:
        with self._lock:
            landed = self._stats["landed"]
            total = self._sent
        rate = landed / total if total else 0.0
        return landed, total, rate

    def _track_results(self) -> None:
        while True:
            try:
                stream = self.client.SubscribeBundleResults(
                    SubscribeBundleResultsRequest()
                )
                for result in stream:
                    kind = result.WhichOneof("result")
                    if kind in ("processed", "finalized"):
                        kind = "landed"
                    with self._lock:
                        if kind not in self._stats or result.bundle_id not in self._own:
                            continue
                        self._own.discard(result.bundle_id)
                        self._stats[kind] += 1
                        count = sum(self._stats.values())
                    if count % self.LOG_EVERY == 0:
                        landed, total, rate = self.land_rate()
                        logger.info(
                            "bundles tracked=%d landed=%d hit=%.1f%%",
                            total,
                            landed,
                            rate * 100,
                        )
            except Exception:
                logger.exception("bundle result stream failed, resubscribing")
                time.sleep(1)
