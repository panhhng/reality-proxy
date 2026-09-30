"use strict";
const element = (selector) => {
    const match = document.querySelector(selector);
    if (!match) {
        throw new Error(`Dashboard element not found: ${selector}`);
    }
    return match;
};
const ui = {
    active: element("#active"),
    accepted: element("#accepted"),
    rejected: element("#rejected"),
    disconnects: element("#disconnects"),
    listen: element("#listen"),
    target: element("#target"),
    latency: element("#latency"),
    bandwidth: element("#bandwidth"),
    dropRate: element("#drop-rate"),
    testLink: element("#test-link"),
    upload: element("#upload"),
    download: element("#download"),
    total: element("#total"),
    uploadBar: element("#upload-bar"),
    downloadBar: element("#download-bar"),
    updated: element("#updated"),
};
function formatBytes(value) {
    if (value < 1000)
        return `${value} B`;
    const units = ["kB", "MB", "GB", "TB"];
    let amount = value;
    let index = -1;
    do {
        amount /= 1000;
        index += 1;
    } while (amount >= 1000 && index < units.length - 1);
    return `${amount.toFixed(amount < 10 ? 1 : 0)} ${units[index]}`;
}
function renderMetrics(data) {
    ui.active.textContent = String(data.active_connections);
    ui.accepted.textContent = String(data.accepted_connections);
    ui.rejected.textContent = String(data.rejected_connections);
    ui.disconnects.textContent = String(data.simulated_disconnects);
    ui.listen.textContent = data.listen_address;
    ui.target.textContent = data.target_address;
    ui.latency.textContent = `${data.latency_ms} ms / chunk`;
    ui.bandwidth.textContent = data.bandwidth_bytes_per_second
        ? `${formatBytes(data.bandwidth_bytes_per_second)}/s per direction`
        : "unlimited";
    ui.dropRate.textContent = `${data.drop_rate_percent}%`;
    ui.testLink.href = `http://${data.listen_address}/`;
    const uploaded = data.client_to_server_bytes;
    const downloaded = data.server_to_client_bytes;
    ui.upload.textContent = formatBytes(uploaded);
    ui.download.textContent = formatBytes(downloaded);
    ui.total.textContent = `${formatBytes(uploaded + downloaded)} total`;
    const maximum = Math.max(uploaded, downloaded, 1);
    ui.uploadBar.style.width = `${(uploaded / maximum) * 100}%`;
    ui.downloadBar.style.width = `${(downloaded / maximum) * 100}%`;
    ui.updated.textContent = `Updated ${new Date().toLocaleTimeString()}`;
}
async function refreshMetrics() {
    try {
        const response = await fetch("/api/metrics", { cache: "no-store" });
        if (!response.ok)
            throw new Error("Metrics unavailable");
        renderMetrics((await response.json()));
    }
    catch {
        ui.updated.textContent = "Metrics connection lost";
    }
}
void refreshMetrics();
window.setInterval(() => void refreshMetrics(), 1000);
