"""Union-Find identity resolution: groups multiple user_pseudo_id that belong to the same real visitor."""
from __future__ import annotations

import pandas as pd


class UnionFind:
    """Weighted quick-union with path compression."""

    def __init__(self):
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[x] != root:
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._rank[ra] < self._rank[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        if self._rank[ra] == self._rank[rb]:
            self._rank[ra] += 1

    def components(self) -> dict[str, str]:
        """Return {element: root} for every known element."""
        return {x: self.find(x) for x in self._parent}


def build_visitor_identity(
    lead_events: pd.DataFrame,
    clients: pd.DataFrame,
) -> pd.DataFrame:
    """Build a mapping (user_pseudo_id → canonical_visitor_id).

    Edges in the identity graph:
      1. user_pseudo_id ↔ phone_clean   (from lead_events where phone is not null)
      2. user_pseudo_id ↔ email         (from lead_events where email is not null)
      3. client_id      ↔ phone_clean   (from clients)
      4. client_id      ↔ email         (from clients)

    Transitivity via Union-Find: if upid_A shares email with client_123,
    and client_123 shares phone with upid_B, then A and B get the same
    canonical_visitor_id.

    Returns a DataFrame with columns:
      - user_pseudo_id
      - canonical_visitor_id   (root of the connected component)
      - identity_cluster_size  (how many user_pseudo_ids share this canonical)
      - identity_match_method  ('self' if singleton, else 'phone', 'email', 'both')
    """
    uf = UnionFind()

    # Namespace prefixes to avoid collisions between IDs and contact values
    P_PHONE = "phone:"
    P_EMAIL = "email:"
    P_CLIENT = "client:"

    # --- Edges from lead_events ---
    if "phone_clean" in lead_events.columns:
        phone_edges = lead_events[lead_events["phone_clean"].notna()][
            ["user_pseudo_id", "phone_clean"]
        ].drop_duplicates()
        for upid, phone in phone_edges.itertuples(index=False, name=None):
            if pd.notna(upid) and pd.notna(phone) and str(phone).strip():
                uf.union(str(upid), P_PHONE + str(phone).strip())
    elif "phone" in lead_events.columns:
        phone_edges = lead_events[lead_events["phone"].notna()][
            ["user_pseudo_id", "phone"]
        ].drop_duplicates()
        for upid, phone in phone_edges.itertuples(index=False, name=None):
            if pd.notna(upid) and pd.notna(phone) and str(phone).strip():
                uf.union(str(upid), P_PHONE + str(phone).strip())

    if "email" in lead_events.columns:
        email_edges = lead_events[
            lead_events["email"].notna()
            & (lead_events["email"].astype(str).str.strip().str.len() > 0)
        ][["user_pseudo_id", "email"]].drop_duplicates()
        for upid, email in email_edges.itertuples(index=False, name=None):
            if pd.notna(upid):
                uf.union(str(upid), P_EMAIL + str(email).strip().lower())

    # --- Edges from clients ---
    if "phone_clean" in clients.columns:
        client_phone = clients[clients["phone_clean"].notna()][
            ["client_id", "phone_clean"]
        ].drop_duplicates()
        for cid, phone in client_phone.itertuples(index=False, name=None):
            if pd.notna(cid) and pd.notna(phone) and str(phone).strip():
                uf.union(P_CLIENT + str(int(cid)), P_PHONE + str(phone).strip())

    if "email" in clients.columns:
        client_email = clients[
            clients["email"].notna()
            & (clients["email"].astype(str).str.strip().str.len() > 0)
        ][["client_id", "email"]].drop_duplicates()
        for cid, email in client_email.itertuples(index=False, name=None):
            if pd.notna(cid):
                uf.union(P_CLIENT + str(int(cid)), P_EMAIL + str(email).strip().lower())

    # --- Build upid -> earliest event_datetime for stable canonical selection ---
    upid_first_seen = (
        lead_events.dropna(subset=["user_pseudo_id"])
        .groupby(lead_events["user_pseudo_id"].dropna().astype(str))["event_datetime"]
        .min()
        .to_dict()
    )

    # --- Extract user_pseudo_id components ---
    all_upids = set(lead_events["user_pseudo_id"].dropna().astype(str).unique())
    # Ensure all upids are in the UF (singletons included)
    for upid in all_upids:
        uf.find(upid)

    comp = uf.components()

    # For each user_pseudo_id, find its root
    upid_to_root: dict[str, str] = {}
    for upid in all_upids:
        upid_to_root[upid] = comp.get(upid, upid)

    # Group by root to find which user_pseudo_ids share a component
    from collections import defaultdict
    root_to_upids: dict[str, list[str]] = defaultdict(list)
    for upid, root in upid_to_root.items():
        root_to_upids[root].append(upid)

    # Pick canonical = user_pseudo_id with the earliest event_datetime in each component
    root_to_canonical: dict[str, str] = {}
    for root, upids in root_to_upids.items():
        root_to_canonical[root] = min(
            upids,
            key=lambda u: upid_first_seen.get(u, pd.Timestamp.max),
        )

    # Determine match method per component
    root_phone: set[str] = set()
    root_email: set[str] = set()
    for node, root in comp.items():
        if node.startswith(P_PHONE):
            root_phone.add(root)
        elif node.startswith(P_EMAIL):
            root_email.add(root)

    rows = []
    for upid in all_upids:
        root = upid_to_root[upid]
        canonical = root_to_canonical[root]
        cluster_size = len(root_to_upids[root])
        has_phone = root in root_phone
        has_email = root in root_email
        if cluster_size == 1 and not has_phone and not has_email:
            method = "self"
        elif has_phone and has_email:
            method = "both"
        elif has_phone:
            method = "phone"
        elif has_email:
            method = "email"
        else:
            method = "self"
        rows.append((upid, canonical, cluster_size, method))

    return pd.DataFrame(
        rows,
        columns=["user_pseudo_id", "canonical_visitor_id", "identity_cluster_size", "identity_match_method"],
    )
