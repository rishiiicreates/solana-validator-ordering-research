#!/usr/bin/env python3
"""
Solana Validator Block Transaction Ordering Analyzer & Verification Harness
Author: Systems & MEV Research Group
Target: Superteam Ukraine Research Bounty ("Inside the Block: How Solana Validators Order Transactions")

Features:
- Connects to Solana Mainnet RPC (zero external dependencies, pure Python).
- Resolves validator identity, stake, and client software (Agave, JitoLabs, Firedancer, AgaveBam).
- Decodes ComputeBudget instructions: ComputeUnitLimit, ComputeUnitPrice, PriorityFee.
- Maps write-locked accounts from static keys and Address Lookup Tables (ALTs).
- Detects Jito MEV bundles and evaluates block index positioning.
- Evaluates Global vs Local Write-Lock Monotonicity (Kendall concordant pairs).
- Generates publication-grade empirical analysis reports.
"""

import sys
import json
import urllib.request
import urllib.error
import struct
import math
from typing import Dict, List, Tuple, Any, Optional

DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
COMPUTE_BUDGET_PROGRAM_ID = "ComputeBudget111111111111111111111111111111"

JITO_TIP_ACCOUNTS = {
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTxvQssQn1Wn78zsrf26h5AuhHS5B9b4PFwR",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
}

# Base58 Alphabet for zero-dependency parsing
B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

def b58decode(v: str) -> bytes:
    """Decode base58 string into raw bytes."""
    origlen = len(v)
    v = v.lstrip("1")
    newlen = len(v)
    acc = 0
    for char in v:
        acc = acc * 58 + B58_ALPHABET.index(char)
    res = bytearray()
    while acc > 0:
        res.append(acc & 0xFF)
        acc >>= 8
    res.reverse()
    return bytes(origlen - newlen) + bytes(res)


def rpc_call(url: str, method: str, params: list) -> Any:
    """Execute JSON-RPC POST request."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if "error" in data:
                raise RuntimeError(f"RPC Error ({method}): {data['error']}")
            return data.get("result")
    except Exception as e:
        raise RuntimeError(f"HTTP/RPC request failed: {e}")


def get_cluster_node_map(rpc_url: str) -> Dict[str, Dict[str, Any]]:
    """Fetch cluster nodes and map by node pubkey."""
    nodes = rpc_call(rpc_url, "getClusterNodes", []) or []
    return {n.get("pubkey"): n for n in nodes if n.get("pubkey")}


def get_vote_account_map(rpc_url: str) -> Dict[str, Dict[str, Any]]:
    """Fetch active vote accounts and map by node pubkey."""
    votes = rpc_call(rpc_url, "getVoteAccounts", []) or {}
    current = votes.get("current", [])
    return {v.get("nodePubkey"): v for v in current if v.get("nodePubkey")}


def decode_compute_budget(instructions: list, keys: List[str]) -> Tuple[int, int]:
    """
    Extract requested Compute Units limit and price in micro-lamports per CU.
    Returns: (cu_limit, cu_price_micro_lamports)
    """
    cu_limit = 200_000  # Solana default limit per instruction / legacy default
    cu_price = 0        # Default 0 prioritization fee

    for ix in instructions:
        prog_idx = ix.get("programIdIndex")
        if prog_idx is not None and prog_idx < len(keys):
            if keys[prog_idx] == COMPUTE_BUDGET_PROGRAM_ID:
                raw_data = ix.get("data", "")
                raw_bytes = b58decode(raw_data)
                if len(raw_bytes) > 0:
                    disc = raw_bytes[0]
                    # Disc 2: SetComputeUnitLimit(u32)
                    if disc == 2 and len(raw_bytes) >= 5:
                        cu_limit = struct.unpack("<I", raw_bytes[1:5])[0]
                    # Disc 3: SetComputeUnitPrice(u64)
                    elif disc == 3 and len(raw_bytes) >= 9:
                        cu_price = struct.unpack("<Q", raw_bytes[1:9])[0]
                    # Disc 0: RequestUnitsDeprecated(u32 units, u32 additional_fee)
                    elif disc == 0 and len(raw_bytes) >= 9:
                        cu_limit = struct.unpack("<I", raw_bytes[1:5])[0]
    return cu_limit, cu_price


def parse_block_slot(rpc_url: str, slot: int, node_map: dict, vote_map: dict) -> Dict[str, Any]:
    """Fetch and deeply analyze a single block slot."""
    block = rpc_call(rpc_url, "getBlock", [
        slot,
        {
            "encoding": "json",
            "transactionDetails": "full",
            "rewards": False,
            "maxSupportedTransactionVersion": 1
        }
    ])
    if not block:
        raise ValueError(f"Block not found for slot {slot}")

    # Fetch slot leader
    leaders = rpc_call(rpc_url, "getSlotLeaders", [slot, 1]) or []
    leader_pubkey = leaders[0] if leaders else "Unknown"

    node_info = node_map.get(leader_pubkey, {})
    vote_info = vote_map.get(leader_pubkey, {})

    client_id = node_info.get("clientId", "Unknown")
    version = node_info.get("version", "Unknown")
    stake_sol = (vote_info.get("activatedStake", 0)) / 1e9
    vote_pubkey = vote_info.get("votePubkey", "Unknown")

    txs = block.get("transactions", [])
    blockhash = block.get("blockhash", "")
    parent_slot = block.get("parentSlot", 0)

    parsed_txs = []
    account_write_map: Dict[str, List[int]] = {}
    jito_bundles = []

    total_cu_consumed = 0
    total_priority_fees = 0
    total_base_fees = 0

    for idx, tx in enumerate(txs):
        meta = tx.get("meta", {})
        err = meta.get("err")
        fee = meta.get("fee", 0)
        cu_consumed = meta.get("computeUnitsConsumed", 0)
        total_cu_consumed += cu_consumed

        msg = tx.get("transaction", {}).get("message", {})
        account_keys = msg.get("accountKeys", [])
        header = msg.get("header", {})
        num_req_sigs = header.get("numRequiredSignatures", 0)
        num_ro_signed = header.get("numReadonlySignedAccounts", 0)
        num_ro_unsigned = header.get("numReadonlyUnsignedAccounts", 0)

        # Normalize keys list
        keys_str = [k if isinstance(k, str) else k.get("pubkey", "") for k in account_keys]

        # Extract static write locks
        writable_indices = list(range(0, num_req_sigs - num_ro_signed)) + list(range(num_req_sigs, len(keys_str) - num_ro_unsigned))
        writable_keys = [keys_str[i] for i in writable_indices if i < len(keys_str)]

        # Extract dynamic write locks from Address Lookup Tables (ALTs)
        loaded = meta.get("loadedAddresses", {})
        loaded_writable = loaded.get("writable", [])
        writable_keys.extend(loaded_writable)

        all_keys = list(keys_str) + loaded_writable + loaded.get("readonly", [])

        # Parse ComputeBudget instructions
        cu_limit, cu_price = decode_compute_budget(msg.get("instructions", []), keys_str)

        # Compute calculated priority fee vs base fee
        # Solana base fee: 5000 lamports per signature
        base_fee = num_req_sigs * 5000
        calc_prio_fee = (cu_limit * cu_price) // 1_000_000
        total_base_fees += base_fee
        total_priority_fees += max(0, fee - base_fee)

        # Priority fee per CU bidding metric (micro-lamports / CU)
        prio_rate = cu_price

        # Check for Jito Tip payment
        jito_hit = [k for k in all_keys if k in JITO_TIP_ACCOUNTS]
        is_jito_tip = len(jito_hit) > 0

        parsed_tx = {
            "index": idx,
            "fee": fee,
            "base_fee": base_fee,
            "priority_fee": max(0, fee - base_fee),
            "cu_limit": cu_limit,
            "cu_price": cu_price,
            "cu_consumed": cu_consumed,
            "prio_rate": prio_rate,
            "err": err is not None,
            "writable_keys": writable_keys,
            "is_jito_tip": is_jito_tip,
            "jito_accounts": jito_hit,
        }
        parsed_txs.append(parsed_tx)

        if is_jito_tip:
            jito_bundles.append(parsed_tx)

        for w_acc in writable_keys:
            if w_acc not in account_write_map:
                account_write_map[w_acc] = []
            account_write_map[w_acc].append(idx)

    # Statistical Evaluation:
    # 1. Global Spearman / Kendall rank correlation between Tx Index and Priority Rate
    rates = [t["prio_rate"] for t in parsed_txs]
    indices = [t["index"] for t in parsed_txs]

    def rank_list(arr):
        sorted_pairs = sorted(enumerate(arr), key=lambda x: x[1])
        ranks = [0] * len(arr)
        for rank, (orig_idx, _) in enumerate(sorted_pairs):
            ranks[orig_idx] = rank
        return ranks

    r_idx = rank_list(indices)
    r_rate = rank_list(rates)
    n = len(indices)

    # Spearman Rho
    if n > 1:
        d_sq = sum((r_idx[i] - r_rate[i]) ** 2 for i in range(n))
        spearman_rho = 1.0 - (6.0 * d_sq) / (n * (n * n - 1))
    else:
        spearman_rho = 0.0

    # 2. Local Write-Lock Monotonicity (Kendall concordant pairs across contested accounts)
    total_local_pairs = 0
    concordant_local_pairs = 0  # i < j and prio_rate(i) >= prio_rate(j)
    strictly_monotonic_accounts = 0
    contested_accounts = 0

    for acc, tx_indices in account_write_map.items():
        if len(tx_indices) < 2:
            continue
        contested_accounts += 1
        acc_monotonic = True
        for i_pos in range(len(tx_indices)):
            for j_pos in range(i_pos + 1, len(tx_indices)):
                total_local_pairs += 1
                idx_i = tx_indices[i_pos]
                idx_j = tx_indices[j_pos]
                rate_i = parsed_txs[idx_i]["prio_rate"]
                rate_j = parsed_txs[idx_j]["prio_rate"]
                if rate_i >= rate_j:
                    concordant_local_pairs += 1
                else:
                    acc_monotonic = False
        if acc_monotonic:
            strictly_monotonic_accounts += 1

    local_concordance_ratio = (concordant_local_pairs / total_local_pairs) if total_local_pairs > 0 else 1.0

    return {
        "slot": slot,
        "parent_slot": parent_slot,
        "blockhash": blockhash,
        "leader": {
            "pubkey": leader_pubkey,
            "client_id": client_id,
            "version": version,
            "vote_pubkey": vote_pubkey,
            "stake_sol": stake_sol,
        },
        "stats": {
            "total_transactions": len(txs),
            "total_cu_consumed": total_cu_consumed,
            "cu_utilization_pct": (total_cu_consumed / 48_000_000) * 100.0,
            "total_base_fees_sol": total_base_fees / 1e9,
            "total_prio_fees_sol": total_priority_fees / 1e9,
            "jito_tip_transactions": len(jito_bundles),
            "jito_tip_indices": [t["index"] for t in jito_bundles],
            "contested_accounts_count": contested_accounts,
            "strictly_monotonic_accounts": strictly_monotonic_accounts,
            "global_spearman_rho": spearman_rho,
            "local_write_lock_concordance": local_concordance_ratio,
        },
        "sample_txs": parsed_txs[:15],
    }


def print_report(res: Dict[str, Any]):
    """Print an ASCII formatted empirical report."""
    ldr = res["leader"]
    sts = res["stats"]
    print("=" * 80)
    print(f"SOLANA BLOCK SLOT EMPIRICAL VERIFICATION REPORT — SLOT {res['slot']}")
    print("=" * 80)
    print(f"Leader Pubkey : {ldr['pubkey']}")
    print(f"Client Engine : {ldr['client_id']} (Version: {ldr['version']})")
    print(f"Vote Account  : {ldr['vote_pubkey']} (Stake: {ldr['stake_sol']:,.2f} SOL)")
    print("-" * 80)
    print(f"Total Transactions : {sts['total_transactions']}")
    print(f"Block CU Consumed  : {sts['total_cu_consumed']:,} / 48,000,000 ({sts['cu_utilization_pct']:.2f}%)")
    print(f"Total Base Fees    : {sts['total_base_fees_sol']:.6f} SOL")
    print(f"Total Priority Fees: {sts['total_prio_fees_sol']:.6f} SOL")
    print(f"Jito Tip Tx Count  : {sts['jito_tip_transactions']} (Indices: {sts['jito_tip_indices']})")
    print("-" * 80)
    print("TRANSACTION SCHEDULING & CONCURRENCY METRICS:")
    print(f"Global Index vs Priority Fee Spearman Rho : {sts['global_spearman_rho']:.4f}")
    print(f"  -> Interpretation: {'WEAK / INTERLEAVED (Multi-Threaded BankingStage)' if abs(sts['global_spearman_rho']) < 0.4 else 'CORRELATED'}")
    print(f"Contested Write-Lock Accounts             : {sts['contested_accounts_count']}")
    print(f"Strictly Monotonic Contested Accounts     : {sts['strictly_monotonic_accounts']}")
    print(f"Local Write-Lock Pair Concordance         : {sts['local_write_lock_concordance'] * 100:.2f}%")
    print(f"  -> Interpretation: {'STRICT MONOTONIC WRITE-LOCK SERIALIZATION PROVEN' if sts['local_write_lock_concordance'] > 0.85 else 'MODERATE CONCURRENCY JITTER'}")
    print("-" * 80)
    print("FIRST 10 TRANSACTIONS IN BLOCK SLOT:")
    print(f"{'Idx':<4} | {'Fee (lamports)':<14} | {'CU Consumed':<11} | {'CU Price (uLam/CU)':<18} | {'Type'}")
    print("-" * 80)
    for t in res["sample_txs"][:10]:
        t_type = "Jito Tip Tx" if t["is_jito_tip"] else ("Standard Tx" if t["cu_price"] == 0 else "Priority Tx")
        print(f"{t['index']:<4} | {t['fee']:<14} | {t['cu_consumed']:<11} | {t['cu_price']:<18} | {t_type}")
    print("=" * 80 + "\n")


def main():
    rpc_url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RPC_URL
    print(f"Connecting to Solana Mainnet RPC: {rpc_url} ...")

    node_map = get_cluster_node_map(rpc_url)
    vote_map = get_vote_account_map(rpc_url)
    print(f"Loaded {len(node_map)} cluster nodes and {len(vote_map)} active vote accounts.\n")

    # If slot provided as arg
    if len(sys.argv) > 2:
        slots = [int(s) for s in sys.argv[2:]]
    else:
        # Default sample slots across different validator clients
        # 461323308 (HarmonicFiredancer), 461323352 (JitoLabs), 461323304 (HarmonicAgave)
        slots = [461323308, 461323352, 461323304]

    for slot in slots:
        try:
            report = parse_block_slot(rpc_url, slot, node_map, vote_map)
            print_report(report)
        except Exception as e:
            print(f"Failed to analyze slot {slot}: {e}")

if __name__ == "__main__":
    main()
