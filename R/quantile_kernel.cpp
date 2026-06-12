// CR-119 §3.3 ensemble-stats kernel.
// Single pass over rows pre-sorted by group: per group computes mean, sd
// (sample, n-1), q17, q83 (type-7, identical to stats::quantile default) and
// non-NA count, for both `value` and `anomaly`. Replaces the slow grouped
// stats::quantile() path (data.table does not GForce-optimize quantile, so the
// whole j falls to a per-group R callback — ~7M calls / 23 min per period).
//
// na.rm semantics: NA and NaN are dropped (matches mean/sd/quantile na.rm=TRUE).
// n==0 -> mean = NaN (matches mean(numeric(0), na.rm=TRUE)), sd/q = NA.
// n==1 -> mean = x, sd = NA (matches stats::sd), q = x.
//
// `grp` must be 1..G, contiguous, ascending (i.e. data sorted by the by-cols
// and grp := .GRP). Caller cbinds the returned stats to unique(keys) in grp order.
#include <Rcpp.h>
#include <vector>
#include <algorithm>
#include <cmath>
using namespace Rcpp;

// type-7 quantile on an already-sorted, non-empty vector
static inline double q7_sorted(const std::vector<double>& x, double p) {
  size_t n = x.size();
  if (n == 1) return x[0];
  double h = (double)(n - 1) * p;
  size_t lo = (size_t)std::floor(h);
  if (lo + 1 >= n) return x[n - 1];
  return x[lo] + (h - lo) * (x[lo + 1] - x[lo]);
}

// [[Rcpp::export]]
DataFrame ens_stats_cpp(NumericVector value, NumericVector anomaly,
                        IntegerVector grp, int G) {
  int N = value.size();
  if (anomaly.size() != N || grp.size() != N) stop("ens_stats_cpp: length mismatch");

  NumericVector mean(G, R_NaN), sd(G, NA_REAL), q17(G, NA_REAL), q83(G, NA_REAL);
  IntegerVector n_models(G, 0);
  NumericVector mean_a(G, R_NaN), sd_a(G, NA_REAL), q17_a(G, NA_REAL), q83_a(G, NA_REAL);

  std::vector<double> v, a;
  int i = 0;
  while (i < N) {
    int g = grp[i];
    int start = i;
    while (i < N && grp[i] == g) i++;          // [start, i) is one group
    int gi = g - 1;                            // grp is 1-indexed
    if (gi < 0 || gi >= G) stop("ens_stats_cpp: grp out of range");

    v.clear(); a.clear();
    v.reserve(i - start); a.reserve(i - start);
    for (int k = start; k < i; k++) {
      double vv = value[k];   if (!ISNAN(vv)) v.push_back(vv);
      double aa = anomaly[k]; if (!ISNAN(aa)) a.push_back(aa);
    }

    int nv = (int)v.size();
    n_models[gi] = nv;
    if (nv >= 1) {
      double s = 0.0; for (double x : v) s += x;
      double m = s / nv; mean[gi] = m;
      if (nv >= 2) {
        double ss = 0.0; for (double x : v) ss += (x - m) * (x - m);
        sd[gi] = std::sqrt(ss / (nv - 1));
      }
      std::sort(v.begin(), v.end());
      q17[gi] = q7_sorted(v, 0.17);
      q83[gi] = q7_sorted(v, 0.83);
    }

    int na = (int)a.size();
    if (na >= 1) {
      double s = 0.0; for (double x : a) s += x;
      double m = s / na; mean_a[gi] = m;
      if (na >= 2) {
        double ss = 0.0; for (double x : a) ss += (x - m) * (x - m);
        sd_a[gi] = std::sqrt(ss / (na - 1));
      }
      std::sort(a.begin(), a.end());
      q17_a[gi] = q7_sorted(a, 0.17);
      q83_a[gi] = q7_sorted(a, 0.83);
    }
  }

  return DataFrame::create(
    _["mean"] = mean, _["sd"] = sd, _["q17"] = q17, _["q83"] = q83,
    _["n_models"] = n_models,
    _["mean_anomaly"] = mean_a, _["sd_anomaly"] = sd_a,
    _["q17_anomaly"] = q17_a, _["q83_anomaly"] = q83_a
  );
}
