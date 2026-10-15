# D-129 A: reads the rounds file of scripts/overhead.sh and, for each
# quantity, the two copies' medians, their difference and each copy's
# spread — defined exactly:
#   difference = |median_A - median_B| / median_A
#   spread     = (max - min) / median, over one copy's rounds
# `detectable` is the smallest effect this run can tell from noise: the
# largest of the three. With -v gate=0.03 it also says whether the host
# passes: difference <= 3 % and both spreads < 3 %, on every quantity.
# A quantity with fewer than 3 rounds in either copy never passes.
# Failover quantities are reported and never gated (D-130 amends D-129 A).
# With -v noisefile=<the noise run's table> it reads an overhead run:
# each difference against that quantity's detectable bound.
# Input lines: "<label> <round> <bench> key=value ...".
BEGIN {
    # Overhead mode: each quantity's detectable bound, from the noise run.
    if (noisefile != "") {
        while ((getline line < noisefile) > 0) {
            m = split(line, w, " +")
            if (m >= 7 && w[7] ~ /%$/) { d = w[7]; sub(/%$/, "", d); noise[w[1]] = (d + 0) / 100 }
        }
    }
}
function median(arr, n,    i, j, t) {
    for (i = 2; i <= n; i++) for (j = i; j > 1 && arr[j - 1] > arr[j]; j--) { t = arr[j]; arr[j] = arr[j - 1]; arr[j - 1] = t }
    return arr[int((n + 1) / 2)]
}
{
    for (f = 4; f <= NF; f++) {
        split($f, kv, "=")
        if (kv[2] == "" || kv[2] == "-") continue
        k = kv[1]
        if (k !~ /^(publishes_per_s|failover_within_1s|failover_p50_ms|failover_p90_ms|pubacks_per_s|p50_ms|p99_ms|cpu_ms_per_1k)$/) continue
        keys[k] = 1
        c = ++count[$1, k]
        v[$1, k, c] = kv[2] + 0
        if (!(($1, k) in lo) || kv[2] + 0 < lo[$1, k]) lo[$1, k] = kv[2] + 0
        if (!(($1, k) in hi) || kv[2] + 0 > hi[$1, k]) hi[$1, k] = kv[2] + 0
    }
}
END {
    pass = 1
    printf "%-16s %12s %12s %10s %9s %9s %11s %s\n", "quantity", "median_A", "median_B", "difference", "spread_A", "spread_B", "detectable", "verdict"
    for (k in keys) {
        n = count["base", k]; for (i = 1; i <= n; i++) a[i] = v["base", k, i]; ma = median(a, n)
        n = count["head", k]; for (i = 1; i <= n; i++) b[i] = v["head", k, i]; mb = median(b, n)
        diff = (ma == 0) ? 0 : (mb - ma) / ma
        ad = diff < 0 ? -diff : diff
        sa = (ma == 0) ? 0 : (hi["base", k] - lo["base", k]) / ma
        sb = (mb == 0) ? 0 : (hi["head", k] - lo["head", k]) / mb
        det = ad; if (sa > det) det = sa; if (sb > det) det = sb
        verdict = ""
        # Fewer than 3 rounds in either copy, or a copy missing: no spread to
        # judge — never accepted, never read as within.
        enough = (count["base", k] >= 3 && count["head", k] >= 3)
        if (k ~ /^failover_/) {
            # A distribution, not gated (D-130): reported, compared by its
            # share within 1 s, p50 and p90.
            verdict = "not gated (D-130)"
        } else if (gate != "") {
            ok = (enough && ad <= gate && sa < gate && sb < gate)
            if (!ok) pass = 0
            verdict = ok ? "within" : (enough ? "TOO NOISY" : "TOO FEW ROUNDS")
        } else if (!(k in noise)) {
            verdict = "no noise bound"
        } else {
            verdict = (ad <= noise[k] ? "within noise" : "beyond noise")
        }
        printf "%-16s %12.3f %12.3f %+9.2f%% %8.2f%% %8.2f%% %10.2f%% %s\n", k, ma, mb, diff * 100, sa * 100, sb * 100, det * 100, verdict
    }
    if (gate != "") print (pass ? "host: ACCEPTED (D-129)" : "host: NOT accepted (D-129): stop here")
    exit (gate != "" && !pass) ? 3 : 0
}
