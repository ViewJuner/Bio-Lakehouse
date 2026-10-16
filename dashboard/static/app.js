const $ = (id) => document.getElementById(id);

document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    $(btn.dataset.tab).classList.add("active");
  });
});

function fmt(n, d = 0) {
  if (n === null || n === undefined) return "—";
  return Number(n).toFixed(d);
}

async function refresh() {
  const [patients, alerts, gold] = await Promise.all([
    fetch("/api/patients").then((r) => r.json()),
    fetch("/api/alerts").then((r) => r.json()),
    fetch("/api/gold").then((r) => r.json()),
  ]);

  const hot = alerts[0];
  const banner = $("banner");
  if (hot) {
    banner.className = "banner bad";
    banner.textContent = `${hot.patient_id}: ${hot.message}`;
  } else {
    banner.className = "banner ok";
    banner.textContent = "Không có cảnh báo mới — pipeline đang chạy.";
  }

  $("cards").innerHTML = patients
    .map((p) => {
      const hotCard = (p.risk_score || 0) >= 40 || (p.heart_rate || 0) > 120;
      return `<article class="card ${hotCard ? "hot" : ""}">
        <h2>${p.full_name}</h2>
        <div class="meta">${p.patient_id} · ${p.device_id || "—"} · ${p.risk_group}</div>
        <div class="metrics">
          <div><span>Nhịp tim</span><strong>${fmt(p.heart_rate)}</strong></div>
          <div><span>SpO2</span><strong>${fmt(p.spo2)}</strong></div>
          <div><span>Huyết áp</span><strong>${fmt(p.systolic_bp)}/${fmt(p.diastolic_bp)}</strong></div>
          <div><span>Risk</span><strong>${fmt(p.risk_score)}</strong></div>
        </div>
      </article>`;
    })
    .join("");

  $("alert-body").innerHTML = alerts
    .map(
      (a) => `<tr>
        <td>${a.ts || ""}</td>
        <td>${a.patient_id}</td>
        <td class="sev-${a.severity}">${a.severity}</td>
        <td>${a.rule_name}</td>
        <td>${a.message}</td>
      </tr>`
    )
    .join("");

  $("gold-body").innerHTML = gold
    .map(
      (g) => `<tr>
        <td>${g.patient_id}</td>
        <td>${g.window_start || ""}</td>
        <td>${fmt(g.avg_hr, 1)}</td>
        <td>${fmt(g.max_hr)}</td>
        <td>${fmt(g.min_spo2)}</td>
        <td>${fmt(g.avg_sbp, 1)}</td>
        <td>${g.sample_count}</td>
      </tr>`
    )
    .join("");
}

async function loadThresholds() {
  const th = await fetch("/api/thresholds").then((r) => r.json());
  const form = $("th-form");
  Object.keys(th).forEach((k) => {
    if (form[k]) form[k].value = th[k];
  });
}

$("th-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const fd = new FormData(ev.target);
  const body = {};
  fd.forEach((v, k) => {
    body[k] = Number(v);
  });
  const res = await fetch("/api/thresholds", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  $("th-status").textContent = res.ok ? "Đã lưu. Rule engine sẽ đọc lại file YAML." : "Lưu thất bại";
});

loadThresholds();
refresh();
setInterval(refresh, 2000);
