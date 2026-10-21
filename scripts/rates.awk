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
# sessions_ended_per_s under churn (DISCONNECTs written); serving_lag_p99
# and serving_lag_max, the worst node's apply lag over the samples where it
# served clients (D-154 A: `L_g`), and serving_lag_time_max_ms, the same
# lag measured in time on the sample clock. After each kill, serving_again: how long
# until the killed node served again and how far behind it was then, as a
# distribution like the hold.
#
# apply_lag in the events stage includes a killed node catching up before
# it serves; serving_lag leaves those samples out. scripts/fence.awk sizes
# `W` from serving_lag.
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
/^serving_lag / {
    p = field($0, "p99") + 0; m = field($0, "max") + 0
    if (p > serving_p99[round]) serving_p99[round] = p
    if (m > serving_max[round]) serving_max[round] = m
    tm = field($0, "time_max_ms") + 0
    if (tm > serving_time[round]) serving_time[round] = tm
}
/^serving_again / {
    again++
    again_ms[again] = field($0, "after_ms")
    again_lag[again] = field($0, "lag_at_serving")
}
/^churn / {
    ended = field($0, "sessions_ended_per_s")
    keep("sessions_ended_per_s", ended)
    # Paced: how much of the asked rate the churners reached.
    rate = field($0, "rate") + 0
    if (rate > 0) keep("churn_rate_reached_pct", ended / rate * 100)
}
/^event / {
    kind = field($0, "kind")
    events[kind]++
    other[kind, events[kind]] = field($0, "max_gap_ms_other_nodes")
    own[kind, events[kind]] = field($0, "max_gap_ms_its_node")
    gave_up[kind] += field($0, "its_clients_gave_up") + 0
    if (!(kind in kinds)) { kinds[kind] = 1; kind_order[++nkinds] = kind }
}

END {
    for (r = 1; r <= round; r++) {
        if (r in lag_p99) keep("apply_lag_p99", lag_p99[r])
        if (r in lag_max) keep("apply_lag_max", lag_max[r])
        if (r in serving_p99) keep("serving_lag_p99", serving_p99[r])
        if (r in serving_max) keep("serving_lag_max", serving_max[r])
        if (r in serving_time) keep("serving_lag_time_max_ms", serving_time[r])
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
            # `-`: no pause of 20 ms or more spanned the event, or no client
            # was left to see one: missing, not counted as short.
            within = 0; m = 0; missing = 0
            for (i = 1; i <= n; i++) {
                x = (side == 1) ? other[kind, i] : own[kind, i]
                if (x == "-" || x == "") { missing++; continue }
                v[++m] = x + 0
                if (v[m] <= 1000) within++
            }
            sort(v, m)
            who = (side == 1) ? "other_nodes" : "its_node"
            if (m == 0) {
                printf "  hold kind=%s clients=%s n=%d missing=%d\n", kind, who, n, missing
                continue
            }
            printf "  hold kind=%s clients=%s n=%d missing=%d p50_ms=%s p90_ms=%s max_ms=%s within_1s=%.0f%%\n", \
                kind, who, n, missing, quantile(v, m, 0.5), quantile(v, m, 0.9), v[m], within / m * 100
        }
        printf "  hold kind=%s its_clients_gave_up=%d\n", kind, gave_up[kind]
    }
    # `-`: the samples ended before the node served again.
    if (again > 0) {
        m = 0; missing = 0
        for (i = 1; i <= again; i++) {
            if (again_ms[i] == "-" || again_ms[i] == "") { missing++; continue }
            m++; t[m] = again_ms[i] + 0; l[m] = again_lag[i] + 0
        }
        sort(t, m); sort(l, m)
        if (m == 0) printf "  serving_again n=%d missing=%d\n", again, missing
        else printf "  serving_again n=%d missing=%d p50_ms=%s p90_ms=%s max_ms=%s lag_p50=%s lag_max=%s\n", \
            again, missing, quantile(t, m, 0.5), quantile(t, m, 0.9), t[m], quantile(l, m, 0.5), l[m]
    }
}
