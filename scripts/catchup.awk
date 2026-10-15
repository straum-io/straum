# D-133 A: medians per read size over the rounds of linux-run.sh step 5.
# Input lines are prefixed "rb=<bytes> round=<n> "; reads catchup_summary
# (lag rate under load, recovery rate), puback (PUBACK p50/p99), diag
# (commit latency, node by node) and resources (CPU, disk, summed over the
# nodes).
function median(arr, n,    i, j, t) {
    for (i = 2; i <= n; i++) for (j = i; j > 1 && arr[j - 1] > arr[j]; j--) { t = arr[j]; arr[j] = arr[j - 1]; arr[j - 1] = t }
    return n ? arr[int((n + 1) / 2)] : "-"
}
function keep(size, key, v) { if (v != "" && v != "-") { c = ++n[size, key]; vals[size, key, c] = v + 0 } }
{
    split($1, a, "="); size = a[2]; split($2, b, "="); round = b[2]
    sizes[size] = 1
    for (f = 3; f <= NF; f++) { split($f, kv, "="); field[kv[1]] = kv[2] }
    if ($3 == "catchup_summary") { keep(size, "lag_rate_under_load", field["lag_rate_under_load"]); if (field["recovery_rate"] == "caught_up") caught[size]++; else keep(size, "recovery_rate", field["recovery_rate"]) }
    if ($3 == "puback") { keep(size, "puback_p50_ms", field["p50_ms"]); keep(size, "puback_p99_ms", field["p99_ms"]) }
    if ($3 == "diag") { keep(size, "commit_p99_ms_node" field["node"], field["p99_ms"]) }
    if ($3 == "resources") { cpu[size, round] += field["cpu_ms"]; if ("disk_write_bytes" in field) disk[size, round] += field["disk_write_bytes"]; rounds[size, round] = 1 }
    delete field
}
END {
    for (k in rounds) { split(k, p, SUBSEP); keep(p[1], "cpu_ms_all_nodes", cpu[p[1], p[2]]); if ((p[1], p[2]) in disk) keep(p[1], "disk_write_bytes_all_nodes", disk[p[1], p[2]]) }
    for (size in sizes) {
        printf "read_bytes=%s", size
        if (size in caught) printf " caught_up_runs=%d", caught[size]
        for (k in n) {
            split(k, p, SUBSEP)
            if (p[1] != size) continue
            m = n[k]; for (i = 1; i <= m; i++) arr[i] = vals[size, p[2], i]
            printf " %s=%s(n=%d)", p[2], median(arr, m), m
        }
        printf "\n"
    }
}
