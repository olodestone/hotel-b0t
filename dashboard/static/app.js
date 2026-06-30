/* Dashboard client behaviour: theme toggle, themed revenue chart, and
   partial (no full-reload) period/staff navigation. */
(function () {
  "use strict";

  function naira(n) { return "₦" + Math.round(n).toLocaleString(); }
  function compact(n) {
    var a = Math.abs(n);
    if (a >= 1e6) return "₦" + (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
    if (a >= 1e3) return "₦" + (n / 1e3).toFixed(0) + "K";
    return "₦" + n;
  }
  function cssvar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }
  function theme() { return document.documentElement.getAttribute("data-theme") || "light"; }

  var chart = null;
  function buildTrend() {
    var el = document.getElementById("trend-data");
    var ctx = document.getElementById("trend");
    if (!el || !ctx || !window.Chart) return;
    var rows = JSON.parse(el.textContent);
    var text = cssvar("--muted"), grid = cssvar("--chart-grid");
    if (chart) chart.destroy();
    chart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: rows.map(function (r) { return r.date.slice(5); }),
        datasets: [
          { label: "Bar", data: rows.map(function (r) { return r.bar; }),
            backgroundColor: cssvar("--c-bar"), borderRadius: 5, maxBarThickness: 24 },
          { label: "Rooms", data: rows.map(function (r) { return r.rooms; }),
            backgroundColor: cssvar("--c-rooms"), borderRadius: 5, maxBarThickness: 24 }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { labels: { color: text, usePointStyle: true, pointStyle: "circle", boxWidth: 8 } },
          tooltip: {
            callbacks: {
              label: function (c) { return c.dataset.label + ": " + naira(c.parsed.y); },
              footer: function (items) {
                var t = items.reduce(function (s, i) { return s + i.parsed.y; }, 0);
                return "Total: " + naira(t);
              }
            }
          }
        },
        scales: {
          x: { stacked: true, grid: { display: false }, ticks: { color: text, maxRotation: 0, autoSkipPadding: 16 } },
          y: { stacked: true, beginAtZero: true, grid: { color: grid, drawBorder: false },
               ticks: { color: text, callback: function (v) { return compact(v); } } }
        }
      }
    });
  }

  function setTheme(t) {
    document.documentElement.setAttribute("data-theme", t);
    try { localStorage.setItem("theme", t); } catch (e) {}
    var b = document.getElementById("theme-btn");
    if (b) { b.textContent = t === "dark" ? "☀️" : "🌙"; b.setAttribute("aria-label", t === "dark" ? "Switch to light" : "Switch to dark"); }
    buildTrend();
  }

  // ── Partial period/staff navigation (swap #period-content, no full reload) ──

  // Build a clean "/?period=…&staff=…" URL from one of the toolbar/staff forms,
  // dropping empty values so the address bar stays tidy.
  function navUrlFromForm(form) {
    var raw = new URLSearchParams(new FormData(form));
    var clean = new URLSearchParams();
    raw.forEach(function (v, k) { if (v !== "") clean.append(k, v); });
    var qs = clean.toString();
    return qs ? "/?" + qs : "/";
  }

  var inFlight = null;
  function loadPeriod(navUrl, push) {
    var container = document.getElementById("period-content");
    if (!container) { window.location.href = navUrl; return; }
    var base = container.getAttribute("data-partial") || "/partial/period";
    var search = "";
    try { search = new URL(navUrl, window.location.origin).search; } catch (e) { search = ""; }

    container.classList.add("is-loading");
    var token = {};
    inFlight = token;
    fetch(base + search, { credentials: "same-origin", headers: { "X-Partial": "1" } })
      .then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.text(); })
      .then(function (html) {
        if (inFlight !== token) return;          // a newer request superseded this one
        if (chart) { chart.destroy(); chart = null; }
        container.innerHTML = html;
        container.classList.remove("is-loading");
        if (push !== false) history.pushState({ period: true }, "", navUrl);
        buildTrend();
      })
      .catch(function () {
        if (inFlight !== token) return;
        window.location.href = navUrl;            // graceful fallback to a full load
      });
  }

  function wirePartialNav() {
    // Period/staff links (quick segments, staff drill-down, clear-filter).
    document.addEventListener("click", function (e) {
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      var a = e.target.closest ? e.target.closest("a.js-period") : null;
      if (!a) return;
      e.preventDefault();
      loadPeriod(a.getAttribute("href"), true);
    });
    // Date / month pickers and the staff <select> (they live in .js-period-form).
    document.addEventListener("change", function (e) {
      var el = e.target;
      if (!el.name || (el.name !== "period" && el.name !== "staff")) return;
      var form = el.closest ? el.closest("form.js-period-form") : null;
      if (!form) return;
      loadPeriod(navUrlFromForm(form), true);
    });
    // Back / forward — re-render the region for the URL without pushing state.
    window.addEventListener("popstate", function () {
      loadPeriod(window.location.pathname + window.location.search, false);
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    var b = document.getElementById("theme-btn");
    if (b) {
      b.textContent = theme() === "dark" ? "☀️" : "🌙";
      b.addEventListener("click", function () { setTheme(theme() === "dark" ? "light" : "dark"); });
    }
    wirePartialNav();
    buildTrend();
  });
})();
