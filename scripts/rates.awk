# What sizes the step 7c fence (straum#23), from one rates-<stage>.txt of
# scripts/linux-host.sh: across its rounds, each quantity's median, min, max
# and spread, (max - min) / median; for events, each kind's hold as a
# distribution (D-130 A, D-134 A). Size from the max, not the median: the
# fence is a bound.
#
#   awk -v stage=steady -f scripts/rates.awk target/linux-host/<time>/rates-steady.txt
#
# Per round: pubacks_per_s, p50_ms and p99_ms (publish to PUBACK, an upper
# bound on commit latency: it includes the gateway and apply);
# records_per_s (log positions per second); apply_lag_p99 and
# apply_lag_max, the worst node's, in positions (the unit `W` counts);
# sessions_ended_per_s under churn (DISCONNECTs written).
#
# In the events stage the worst node includes one killed and catching up:
# it serves no client meanwhile, so it proposes nothing; size `W` from the
# steady and heavy stages, and the hold from the events stage.
function field(line, key,    n, i, kv) {
    n = split(line, parts, " ")
    for (i = 1; i <= n; i++) {
        split(parts[i], kv, "=")
        if (kv[1] == key) return kv[2]
    }
    return ""
}

# Sorts a[1..n] in place.
function sort(a, n,    i, j, t) {
    for (i = 2; i <= n; i++) {
        t = a[i]
        for (j = i - 1; j >= 1 && a[j] > t; j--) a[j + 1] = a[j]
        a[j + 1] = t
    }
}

function quantile(a, n, q,    i) {
    i = int(n * q) + 1
    if (i > n) i = n
    return a[i]
}

function keep(key, value) {
    if (value == "" || value == "-") return
    count[key]++
    values[key, count[key]] = value + 0
    if (!(key in seen)) { seen[key] = 1; order[++keys] = key }
}

/^--- round/ { round++; worst_p99 = 0; worst_max = 0 }
/^puback / {
    keep("pubacks_per_s", field($0, "pubacks_per_s"))
    keep("p50_ms", field($0, "p50_ms"))
    keep("p99_ms", field($0, "p99_ms"))
}
/^positions / { keep("records_per_s", field($0, "records_per_s")) }
/^apply_lag / {
    p = field($0, "p99") + 0; m = field($0, "max") + 0
    if (p > lag_p99[round]) lag_p99[round] = p
    if (m > lag_max[round]) lag_max[round] = m
}
/^churn / { keep("sessions_ended_per_s", field($0, "sessions_ended_per_s")) }
/^event / {
    kind = field($0, "kind")
    events[kind]++
    other[kind, events[kind]] = field($0, "max_gap_ms_other_nodes") + 0
    own[kind, events[kind]] = field($0, "max_gap_ms_its_node") + 0
    if (!(kind in kinds)) { kinds[kind] = 1; kind_order[++nkinds] = kind }
}

END {
    for (r = 1; r <= round; r++) {
        if (r in lag_p99) keep("apply_lag_p99", lag_p99[r])
        if (r in lag_max) keep("apply_lag_max", lag_max[r])
    }
    print "stage=" stage " rounds=" round
    for (k = 1; k <= keys; k++) {
        key = order[k]; n = count[key]
        for (i = 1; i <= n; i++) v[i] = values[key, i]
        sort(v, n)
        median = quantile(v, n, 0.5)
        spread = (median > 0) ? (v[n] - v[1]) / median * 100 : 0
        printf "  %s median=%s min=%s max=%s spread=%.1f%% n=%d\n", key, median, v[1], v[n], spread, n
    }
    for (k = 1; k <= nkinds; k++) {
        kind = kind_order[k]; n = events[kind]
        for (side = 1; side <= 2; side++) {
            within = 0
            for (i = 1; i <= n; i++) {
                v[i] = (side == 1) ? other[kind, i] : own[kind, i]
                if (v[i] <= 1000) within++
            }
            sort(v, n)
            who = (side == 1) ? "other_nodes" : "its_node"
            printf "  hold kind=%s clients=%s n=%d p50_ms=%s p90_ms=%s max_ms=%s within_1s=%.0f%%\n", \
                kind, who, n, quantile(v, n, 0.5), quantile(v, n, 0.9), v[n], within / n * 100
        }
    }
}
