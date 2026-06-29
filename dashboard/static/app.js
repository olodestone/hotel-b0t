/* Dashboard client behaviour: theme toggle + themed revenue chart. */
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

  document.addEventListener("DOMContentLoaded", function () {
    var b = document.getElementById("theme-btn");
    if (b) {
      b.textContent = theme() === "dark" ? "☀️" : "🌙";
      b.addEventListener("click", function () { setTheme(theme() === "dark" ? "light" : "dark"); });
    }
    buildTrend();
  });
})();
