# The step 7c fence's sizes from scripts/rates.awk's summary (D-154 A,
# straum#29). Preliminary always: the inputs are one host's maxima, valid
# only once that host passes D-129, and ×4 is an engineering margin, not a
# proven bound.
#
#   awk -f scripts/fence.awk target/linux-host/<time>/summary-rates.txt
#
# W >= (L_g + C×R + H×R) × 4, in log positions:
#   L_g  the largest serving_lag_max: the lag a gateway proposes with;
#   C    the largest PUBACK p99, in seconds: an upper bound on commit
#        latency (it includes the gateway, apply and the client's queue);
#   R    the largest records_per_s;
#   H    the largest hold across a handoff or kill for the other nodes'
#        clients, in seconds;
# all from the steady, heavy and events stages. The churn stages starve
# publishes (straum#26), so they give the removal rate only.
#
# The tombstone queue, apart from W and from R: removals per second (the
# largest sessions_ended_per_s of any churn stage) over the longest a
# proposer can trail in time, T = T_g + C + H, × 4, with T_g the largest
# serving_lag_time_max_ms of any stage, churn included (removals come from
# there), measured on the sampler's clock. Pruning drops tombstones more
# than W positions old, so at that removal rate the queue never uses more
# than removals × W / R: printed to check against, with
# `above_pruning_bound` when the queue is larger, that space unused. A
# record can remove more than one session (a node's reset, a publish that
# overflows detached sessions), which this rate does not count.
#
# Both rounded up to a power of two.
function field(line, key,    n, i, kv) {
    n = split(line, parts, " ")
    for (i = 1; i <= n; i++) {
        split(parts[i], kv, "=")
        if (kv[1] == key) return kv[2]
    }
    return ""
}

function pow2(x,    p) {
    p = 1
    while (p < x) p *= 2
    return p
}

function most(name, value) {
    if (value == "" || value == "-") return
    if (!(name in best) || value + 0 > best[name]) best[name] = value + 0
}

/^stage=/ {
    stage = field($0, "stage")
    sizing = (stage == "steady" || stage == "heavy" || stage == "events")
}
sizing && $1 == "serving_lag_max" { most("L", field($0, "max")) }
$1 == "serving_lag_time_max_ms" { most("T", field($0, "max") / 1000) }
sizing && $1 == "p99_ms" { most("C", field($0, "max") / 1000) }
sizing && $1 == "records_per_s" { most("R", field($0, "max")) }
sizing && $1 == "hold" && field($0, "clients") == "other_nodes" { most("H", field($0, "max_ms") / 1000) }
$1 == "sessions_ended_per_s" { most("removals", field($0, "max")) }

END {
    print "PRELIMINARY (D-154 A): valid only on a host that passes D-129; x4 is not a proven bound"
    missing = ""
    split("L T C R H removals", need, " ")
    for (i = 1; i <= 6; i++) if (!(need[i] in best)) missing = missing " " need[i]
    if (missing != "") {
        print "not computed: no" missing " in the summary (serving_lag needs a puback built at straum#29 or later)"
        exit 1
    }
    L = best["L"]; C = best["C"]; R = best["R"]; H = best["H"]
    if (R <= 0) { print "not computed: records_per_s is 0"; exit 1 }
    w = (L + C * R + H * R) * 4
    printf "inputs L_g=%.0f T_g_s=%.3f C_s=%.3f R=%.0f H_s=%.3f removals_per_s=%.0f\n", L, best["T"], C, R, H, best["removals"]
    printf "W raw=%.0f (L_g %.0f + C*R %.0f + H*R %.0f, x4) pow2=%d\n", w, L, C * R, H * R, pow2(w)
    t = best["T"] + C + H
    q = best["removals"] * t * 4
    bound = best["removals"] * pow2(w) / R
    printf "queue raw=%.0f (removals %.0f/s x T %.3f s, x4) pow2=%d pruning_bound=%.0f%s\n", \
        q, best["removals"], t, pow2(q), bound, (q > bound) ? " above_pruning_bound" : ""
}
