// Single-pass Theil–Sen + Mann–Kendall kernel for R/2.1 §3.4 (Speedup #3).
//
// Replaces the two independent O(n^2) Kendall passes done by trend::sens.slope()
// (slope + CI) and trend::mk.test() (p-value) with ONE pairwise pass that yields
// Sen slope, Sen CI, Kendall S, tie-corrected varS, and the two-sided MK p-value.
//
// Numerically IDENTICAL to trend 1.1.6 by construction — replicates verbatim:
//   .mkScore : S = sum_{i<j} sign(x[j]-x[i])
//   .varmk   : varS = (n(n-1)(2n+5) - sum t(t-1)(2t+5)) / 18      (t = tie-group sizes)
//   sens.slope: d[k]=(x[j]-x[i])/(j-i); b=median(d);
//               C=qnorm(1-(1-cl)/2)*sqrt(varS);
//               rank.up=round((k+C)/2+1); rank.lo=round((k-C)/2);
//               lo=sort(d)[rank.lo]; up=sort(d)[rank.up]
//   z = sign(S)*(|S|-1)/sqrt(varS);  pval = 2*min(0.5, pnorm(|z|, lower=FALSE))
//
// Slopes use the time INDEX (1..n), exactly like trend::sens.slope (NOT the `year`
// column). The baseline-dependent intercept is computed outside, in data.table.
#include <Rcpp.h>
#include <algorithm>
#include <cmath>
#include <vector>
using namespace Rcpp;

// [[Rcpp::export]]
List mk_sen_cpp(NumericVector x, double conf_level = 0.95) {
  const int n = x.size();
  if (n < 4) {
    return List::create(_["slope"]=NA_REAL, _["intercept"]=NA_REAL,
                        _["ci_low"]=NA_REAL, _["ci_high"]=NA_REAL,
                        _["p_value"]=NA_REAL, _["S"]=NA_REAL, _["varS"]=NA_REAL,
                        _["z"]=NA_REAL, _["ok"]=false);
  }
  // --- single pairwise pass: Kendall S + pairwise slopes ---
  const long k = (long)n * (n - 1) / 2;
  std::vector<double> d;
  d.reserve(k);
  double S = 0.0;
  for (int i = 0; i < n - 1; ++i) {
    const double xi = x[i];
    for (int j = i + 1; j < n; ++j) {
      const double diff = x[j] - xi;
      S += (diff > 0.0) - (diff < 0.0);          // sign(diff)
      d.push_back(diff / (double)(j - i));
    }
  }
  // --- tie-corrected varS (replicates table(x) + .varmk) ---
  std::vector<double> xs(x.begin(), x.end());
  std::sort(xs.begin(), xs.end());
  double tadjs = 0.0;
  long run = 1;
  for (size_t i = 1; i <= xs.size(); ++i) {
    if (i < xs.size() && xs[i] == xs[i - 1]) { ++run; }
    else { const double t = (double)run; tadjs += t * (t - 1.0) * (2.0 * t + 5.0); run = 1; }
  }
  const double varS = ((double)n * (n - 1) * (2 * n + 5) - tadjs) / 18.0;

  // --- Sen slope = median(d), and CI from sorted d ---
  std::sort(d.begin(), d.end());
  double b_sen;
  if (k % 2 == 1) b_sen = d[k / 2];
  else            b_sen = (d[k / 2 - 1] + d[k / 2]) / 2.0;

  const double C = R::qnorm(1.0 - (1.0 - conf_level) / 2.0, 0.0, 1.0, 1, 0) * std::sqrt(varS);
  // R's round() is round-half-to-even; nearbyint() under default FE_TONEAREST matches.
  const long rank_up = (long) std::nearbyint((k + C) / 2.0 + 1.0);
  const long rank_lo = (long) std::nearbyint((k - C) / 2.0);
  const double lo = (rank_lo >= 1 && rank_lo <= k) ? d[rank_lo - 1] : NA_REAL;
  const double up = (rank_up >= 1 && rank_up <= k) ? d[rank_up - 1] : NA_REAL;

  // --- MK z + two-sided p (continuity correction; matches mk.test default) ---
  // Degenerate all-tied series: varS==0 -> trend gives z=0*(-1)/0=NaN, p=NaN. Match it.
  double z, pval;
  if (varS <= 0.0) {
    z = NA_REAL; pval = NA_REAL;
  } else {
    const double sg = (S > 0.0) - (S < 0.0);
    z = sg * (std::fabs(S) - 1.0) / std::sqrt(varS);
    pval = 2.0 * std::min(0.5, R::pnorm(std::fabs(z), 0.0, 1.0, 0, 0)); // lower.tail=FALSE
  }

  return List::create(_["slope"]=b_sen, _["ci_low"]=lo, _["ci_high"]=up,
                      _["p_value"]=pval, _["S"]=S, _["varS"]=varS, _["z"]=z, _["ok"]=true);
}

// Lag-1 autocorrelation of detrended residuals (biased estimator), matching the R
// computation in yue_tfpw(): r = sum(d[-n]*d[-1]) / sum(d*d), d = detr - mean(detr).
// Provided so the whole TFPW+fit path can run without per-group R overhead.
// [[Rcpp::export]]
double lag1_ac_cpp(NumericVector detr) {
  const int n = detr.size();
  if (n < 2) return 0.0;
  double m = 0.0;
  for (int i = 0; i < n; ++i) m += detr[i];
  m /= n;
  double num = 0.0, den = 0.0;
  std::vector<double> dd(n);
  for (int i = 0; i < n; ++i) { dd[i] = detr[i] - m; den += dd[i] * dd[i]; }
  for (int i = 0; i < n - 1; ++i) num += dd[i] * dd[i + 1];
  return (den > 0.0) ? num / den : 0.0;
}
