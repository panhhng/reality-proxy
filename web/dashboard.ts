type MetricsSnapshot = {
    active_connections: number;
    accepted_connections: number;
    rejected_connections: number;
    simulated_disconnects: number;
    client_to_server_bytes: number;
    server_to_client_bytes: number;
    listen_address: string;
    target_address: string;
    latency_ms: number;
    bandwidth_bytes_per_second: number | null;
    drop_rate_percent: number;
};

const element = <T extends HTMLElement>(selector: string): T => {
    const match = document.querySelector<T>(selector);
    if (!match) {
        throw new Error(`Dashboard element not found: ${selector}`);
    }
    return match;
};

const ui = {
    active: element<HTMLElement>("#active"),
    accepted: element<HTMLElement>("#accepted"),
    rejected: element<HTMLElement>("#rejected"),
    disconnects: element<HTMLElement>("#disconnects"),
    listen: element<HTMLElement>("#listen"),
    target: element<HTMLElement>("#target"),
    latency: element<HTMLElement>("#latency"),
    bandwidth: element<HTMLElement>("#bandwidth"),
    dropRate: element<HTMLElement>("#drop-rate"),
    testLink: element<HTMLAnchorElement>("#test-link"),
    upload: element<HTMLElement>("#upload"),
    download: element<HTMLElement>("#download"),
    total: element<HTMLElement>("#total"),
    uploadBar: element<HTMLElement>("#upload-bar"),
    downloadBar: element<HTMLElement>("#download-bar"),
    updated: element<HTMLElement>("#updated"),
};

function formatBytes(value: number): string {
    if (value < 1000) return `${value} B`;
    const units = ["kB", "MB", "GB", "TB"];
    let amount = value;
    let index = -1;
    do {
        amount /= 1000;
        index += 1;
    } while (amount >= 1000 && index < units.length - 1);
    return `${amount.toFixed(amount < 10 ? 1 : 0)} ${units[index]}`;
}

function renderMetrics(data: MetricsSnapshot): void {
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

async function refreshMetrics(): Promise<void> {
    try {
        const response = await fetch("/api/metrics", { cache: "no-store" });
        if (!response.ok) throw new Error("Metrics unavailable");
        renderMetrics((await response.json()) as MetricsSnapshot);
    } catch {
        ui.updated.textContent = "Metrics connection lost";
    }
}

void refreshMetrics();
window.setInterval(() => void refreshMetrics(), 1000);
