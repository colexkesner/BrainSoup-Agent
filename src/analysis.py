from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.ensemble import IsolationForest
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from statsmodels.nonparametric.smoothers_lowess import lowess


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - series.mean()) / std


def snapshot_2023_rankings(df: pd.DataFrame, weights: dict[str, float], top_n: int = 10) -> pd.DataFrame:
    snap = df[df["year"] == 2023].copy()
    snap["rank_ALICE_pct"] = snap["ALICE_pct"].rank(method="min", ascending=False)
    snap["rank_Poverty_pct"] = snap["Poverty_pct"].rank(method="min", ascending=False)
    snap["priority_score_z"] = _zscore(snap["ALICE_pct"]) + _zscore(snap["Poverty_pct"])
    snap["priority_score_weighted"] = (
        weights.get("alice_pct", 0.35) * snap["ALICE_pct"]
        + weights.get("poverty_pct", 0.35) * snap["Poverty_pct"]
        + weights.get("log_alice_households", 0.15) * np.log1p(snap["ALICE Households"])
        + weights.get("log_poverty_households", 0.15) * np.log1p(snap["Poverty Households"])
    )
    snap["pareto_frontier"] = _pareto_flags(snap[["ALICE_pct", "Poverty_pct"]].to_numpy())
    return snap.sort_values("priority_score_weighted", ascending=False)


def _pareto_flags(values: np.ndarray) -> list[bool]:
    n = len(values)
    flags = []
    for i in range(n):
        v = values[i]
        dominated = np.any(np.all(values >= v, axis=1) & np.any(values > v, axis=1))
        flags.append(not dominated)
    return flags


def compute_trends(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, grp in df.sort_values(["fips", "year"]).groupby("fips"):
        g = grp.sort_values("year")
        for metric in ["ALICE_pct", "Poverty_pct"]:
            vals = g[metric].values
            years = g["year"].values
            deltas = np.diff(vals, prepend=np.nan)
            slope = np.polyfit(years, vals, 1)[0] if len(vals) > 1 else 0.0
            vol = float(np.nanstd(vals))
            roll = pd.Series(vals).rolling(3, min_periods=1).mean().iloc[-1]
            lw = lowess(vals, years, frac=0.8, return_sorted=False)[-1] if len(vals) >= 3 else vals[-1]
            rows.append({
                "fips": g["fips"].iloc[0],
                "County": g["County"].iloc[0],
                "State": g["State"].iloc[0],
                "metric": metric,
                "latest_value": vals[-1],
                "delta_prev_available": deltas[-1],
                "slope": slope,
                "volatility": vol,
                "rolling3_latest": float(roll),
                "lowess_latest": float(lw),
                "trend_direction": "up" if slope > 0 else "down" if slope < 0 else "flat",
            })
    return pd.DataFrame(rows)


def build_rank_changes(df_5yr: pd.DataFrame) -> pd.DataFrame:
    out = []
    for metric in ["ALICE_pct", "Poverty_pct"]:
        tmp = df_5yr.copy()
        tmp["rank"] = tmp.groupby("year")[metric].rank(method="min", ascending=False)
        first_year = tmp["year"].min()
        last_year = tmp["year"].max()
        a = tmp[tmp["year"] == first_year][["fips", "County", "rank"]].rename(columns={"rank": "rank_first"})
        b = tmp[tmp["year"] == last_year][["fips", "rank"]].rename(columns={"rank": "rank_last"})
        m = a.merge(b, on="fips", how="inner")
        m["metric"] = metric
        m["rank_delta"] = m["rank_last"] - m["rank_first"]
        out.append(m)
    return pd.concat(out, ignore_index=True)


def pca_and_cluster(snapshot_2023: pd.DataFrame, seed: int, pca_components: int = 3) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    feat_cols = [
        "ALICE_pct", "Poverty_pct", "Gap_pct", "Above_ALICE_pct",
        "ALICE Threshold - HH under 65", "ALICE Threshold - HH 65 years and over",
    ]
    x = snapshot_2023[feat_cols].copy()
    x["log_households"] = np.log1p(snapshot_2023["Households"])
    x["log_alice_households"] = np.log1p(snapshot_2023["ALICE Households"])
    scaler = StandardScaler()
    z = scaler.fit_transform(x)

    pca = PCA(n_components=min(max(2, pca_components), z.shape[1]), random_state=seed)
    pcs = pca.fit_transform(z)
    loadings = pd.DataFrame(pca.components_.T, index=x.columns, columns=[f"PC{i+1}" for i in range(pca.n_components_)])
    loadings["explained_variance_ratio"] = list(pca.explained_variance_ratio_) + [np.nan] * (len(loadings) - len(pca.explained_variance_ratio_))

    best = {"algo": "kmeans", "k": 2, "silhouette": -1.0, "db": np.inf}
    best_labels = None
    for k in range(2, 9):
        km = KMeans(n_clusters=k, random_state=seed, n_init=20)
        labels = km.fit_predict(z)
        sil = silhouette_score(z, labels)
        db = davies_bouldin_score(z, labels)
        if sil > best["silhouette"] or (np.isclose(sil, best["silhouette"]) and db < best["db"]):
            best = {"algo": "kmeans", "k": k, "silhouette": float(sil), "db": float(db)}
            best_labels = labels
        ag = AgglomerativeClustering(n_clusters=k, linkage="ward")
        al = ag.fit_predict(z)
        sil2 = silhouette_score(z, al)
        db2 = davies_bouldin_score(z, al)
        if sil2 > best["silhouette"] or (np.isclose(sil2, best["silhouette"]) and db2 < best["db"]):
            best = {"algo": "agglomerative", "k": k, "silhouette": float(sil2), "db": float(db2)}
            best_labels = al

    cluster_df = snapshot_2023[["fips", "County", "State", "year"]].copy()
    cluster_df["cluster_label"] = best_labels
    cluster_df["PC1"] = pcs[:, 0]
    cluster_df["PC2"] = pcs[:, 1]
    centroids = cluster_df.groupby("cluster_label")[["PC1", "PC2"]].transform("mean")
    cluster_df["distance_to_cluster_center"] = np.sqrt((cluster_df["PC1"] - centroids["PC1"]) ** 2 + (cluster_df["PC2"] - centroids["PC2"]) ** 2)
    return cluster_df, loadings.reset_index(names="feature"), best


def find_anomalies(df: pd.DataFrame, seed: int) -> pd.DataFrame:
    tmp = df.sort_values(["fips", "year"]).copy()
    tmp["delta_ALICE_pct"] = tmp.groupby("fips")["ALICE_pct"].diff()
    tmp["delta_Poverty_pct"] = tmp.groupby("fips")["Poverty_pct"].diff()
    feat = tmp[["delta_ALICE_pct", "delta_Poverty_pct", "Gap_pct"]].fillna(0.0)
    model = IsolationForest(random_state=seed, contamination=0.05)
    pred = model.fit_predict(feat)
    score = model.decision_function(feat)
    tmp["is_anomaly"] = pred == -1
    tmp["anomaly_score"] = -score
    tmp["reason"] = np.where(tmp["delta_ALICE_pct"].abs() >= tmp["delta_Poverty_pct"].abs(), "ALICE_pct shift dominates", "Poverty_pct shift dominates")
    return tmp[tmp["is_anomaly"]][["fips", "County", "State", "year", "anomaly_score", "reason", "delta_ALICE_pct", "delta_Poverty_pct", "Gap_pct"]].sort_values("anomaly_score", ascending=False)


def build_peers(snapshot_2023: pd.DataFrame, clusters_2023: pd.DataFrame, target_county: str = "Bulloch", n_neighbors: int = 10) -> pd.DataFrame:
    feat_cols = ["ALICE_pct", "Poverty_pct", "Gap_pct", "Above_ALICE_pct"]
    x = snapshot_2023[feat_cols].fillna(0.0)
    z = StandardScaler().fit_transform(x)
    nn = NearestNeighbors(n_neighbors=min(n_neighbors + 1, len(snapshot_2023)))
    nn.fit(z)
    idx = snapshot_2023[snapshot_2023["County"].str.lower() == target_county.lower()].index
    if len(idx) == 0:
        return pd.DataFrame(columns=["target_county", "peer_county", "distance", "shared_cluster"])
    dists, inds = nn.kneighbors(z[idx[0]].reshape(1, -1))
    rows = []
    target_cluster = int(clusters_2023.loc[idx[0], "cluster_label"])
    for dist, i in zip(dists[0], inds[0]):
        if i == idx[0]:
            continue
        rows.append({
            "target_county": target_county,
            "peer_county": snapshot_2023.loc[i, "County"],
            "distance": float(dist),
            "shared_cluster": int(clusters_2023.loc[i, "cluster_label"]) == target_cluster,
        })
    return pd.DataFrame(rows)
