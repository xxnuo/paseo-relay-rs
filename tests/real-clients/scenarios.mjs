import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { pathToFileURL, fileURLToPath } from "node:url";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { randomBytes, createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { monitorEventLoopDelay } from "node:perf_hooks";

const source = path.resolve(process.env.PASEO_SOURCE);
const work = path.resolve(process.env.PASEO_TEST_WORK);
const output = path.resolve(process.env.PASEO_TEST_OUTPUT);
const endpoint = process.env.PASEO_RELAY_ENDPOINT;
const require = createRequire(path.join(source, "package.json"));
const { register } = require("tsx/esm/api");
const imported = new Set();
const config = path.join(work, "tsconfig.json");
await writeFile(
  config,
  JSON.stringify({
    compilerOptions: {
      baseUrl: source,
      paths: {
        "@getpaseo/relay": ["packages/relay/src/index.ts"],
        "@getpaseo/relay/*": ["packages/relay/src/*"],
        "@getpaseo/protocol/*": ["packages/protocol/src/*"],
        "@getpaseo/client": ["packages/client/src/index.ts"],
        "@getpaseo/client/node": ["packages/client/src/node/index.ts"],
        "@getpaseo/client/internal/*": ["packages/client/src/*"],
      },
    },
  }),
);
register({ tsconfig: config, onImport: (url) => imported.add(url) });
const load = (relative) => import(pathToFileURL(path.join(source, relative)).href);
const { createPaseoDaemon } = await load("packages/server/src/server/bootstrap.ts");
const { DaemonClient, getDaemonAuthFailureReason } = await load(
  "packages/client/src/daemon-client.ts",
);
const { generateLocalPairingOffer } = await load("packages/server/src/server/pairing-offer.ts");
const { parseConnectionOfferFromUrl } = await load("packages/protocol/src/connection-offer.ts");
const { buildRelayWebSocketUrl } = await load("packages/protocol/src/daemon-endpoints.ts");
const { generateKeyPair, exportPublicKey, createClientChannel } = await load(
  "packages/relay/src/e2ee.ts",
);
const WebSocket = require("ws");
const pino = require("pino");
const { hashSync } = require("bcryptjs");
const logger = pino({ level: "silent" });
const count = Number(process.env.PASEO_STRESS_CLIENTS ?? 32);
const seconds = Number(process.env.PASEO_STRESS_SECONDS ?? 30);
const password = randomBytes(24).toString("hex");
const clients = [];
const daemons = [];
const sockets = new Set();
const wire = { upgrades: 0, frames: 0, binary: 0, hello: 0, ready: 0, plaintextLeaks: 0 };
const results = [];
const loopDelay = monitorEventLoopDelay({ resolution: 10 });
loopDelay.enable();
const cpuStart = process.cpuUsage();
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const emit = (event) => console.log(JSON.stringify(event));
async function until(check, label, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    assert(Date.now() < deadline, `deadline: ${label}`);
    await delay(20);
  }
}
async function phase(name, action) {
  emit({ phase: name, status: "running" });
  const started = performance.now();
  const details = await action();
  results.push({ phase: name, seconds: (performance.now() - started) / 1000, ...details });
  emit({ ...results.at(-1), status: "passed" });
}
function inspect(data, binary) {
  const bytes = Buffer.from(
    typeof data === "string" ? data : data instanceof ArrayBuffer ? new Uint8Array(data) : data,
  );
  wire.frames++;
  wire.binary += Number(binary);
  if (bytes.includes(password)) wire.plaintextLeaks++;
  if (!binary) {
    try {
      const m = JSON.parse(bytes.toString());
      if (m.type === "e2ee_hello") wire.hello++;
      if (m.type === "e2ee_ready") wire.ready++;
    } catch {}
  }
}
function makeClient(daemon, index, options = {}) {
  const physical = [];
  const modes = [];
  const browser = index % 2 === 0;
  const client = new DaemonClient({
    url: buildRelayWebSocketUrl({
      endpoint,
      useTls: false,
      serverId: daemon.offer.serverId,
      role: "client",
    }),
    clientId: `stress_${index}_${randomBytes(6).toString("hex")}`,
    clientType: browser ? "browser" : index % 3 ? "mobile" : "cli",
    password: options.password ?? password,
    logger,
    connectTimeoutMs: 10000,
    e2ee: {
      enabled: true,
      minimumMode: "protected",
      daemonPublicKeyB64: options.key ?? daemon.offer.daemonPublicKeyB64,
      onSecurityMode: (mode) => modes.push(mode),
    },
    reconnect: { enabled: options.reconnect ?? true, baseDelayMs: 100, maxDelayMs: 1000 },
    webSocketFactory: (url, opts) => {
      const u = new URL(url);
      assert.equal(u.searchParams.get("v"), "2");
      assert(!u.searchParams.has("password") && !u.searchParams.has("token"));
      assert(
        !Object.keys(opts?.headers ?? {}).some((key) => key.toLowerCase() === "authorization") &&
          !opts?.protocols?.length,
      );
      wire.upgrades++;
      const ws = browser ? new globalThis.WebSocket(url) : new WebSocket(url);
      sockets.add(ws);
      physical.push(ws);
      const send = ws.send.bind(ws);
      ws.send = (data) => {
        inspect(data, typeof data !== "string");
        return send(data);
      };
      if (browser) {
        ws.addEventListener("message", (event) =>
          inspect(event.data, typeof event.data !== "string"),
        );
        ws.addEventListener("close", () => sockets.delete(ws));
      } else {
        ws.on("message", inspect);
        ws.on("close", () => sockets.delete(ws));
      }
      return ws;
    },
  });
  const entry = { client, daemon, physical, modes, browser };
  clients.push(entry);
  return entry;
}
async function legacyConnection(daemon, allowed) {
  const ws = new WebSocket(
    buildRelayWebSocketUrl({
      endpoint,
      useTls: false,
      serverId: daemon.offer.serverId,
      role: "client",
    }),
  );
  sockets.add(ws);
  const messages = [];
  let closed = false,
    readyEnvelope;
  const transport = {
    send: (data) => {
      inspect(data, typeof data !== "string");
      ws.send(data);
    },
    close: (code, reason) => ws.close(code, reason),
    onmessage: null,
    onclose: null,
    onerror: null,
  };
  ws.on("message", (data, isBinary) => {
    inspect(data, isBinary);
    if (!isBinary) {
      try {
        const m = JSON.parse(data.toString());
        if (m.type === "e2ee_ready") readyEnvelope = m;
      } catch {}
    }
    transport.onmessage?.({
      data: isBinary
        ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
        : data.toString(),
      isBinary,
    });
  });
  ws.on("close", (code, reason) => {
    closed = true;
    sockets.delete(ws);
    transport.onclose?.(code, reason.toString());
  });
  ws.on("error", (error) => transport.onerror?.(error));
  let channel;
  try {
    await until(() => ws.readyState === WebSocket.OPEN, "legacy TCP upgrade");
    channel = await createClientChannel(transport, daemon.offer.daemonPublicKeyB64, {
      onmessage: (data) => {
        if (typeof data === "string") messages.push(JSON.parse(data));
      },
    });
    await until(() => channel.isOpen() || closed, "legacy handshake policy");
    if (!allowed) {
      assert(closed && !channel.isOpen());
      return;
    }
    assert(channel.isOpen());
    assert(readyEnvelope && readyEnvelope.protected === undefined);
    await channel.send(
      JSON.stringify({
        type: "hello",
        clientId: "legacy-real-client",
        clientType: "cli",
        protocolVersion: 1,
        auth: { kind: "password", password },
      }),
    );
    await until(
      () =>
        messages.some((m) => m.type === "session" && m.message?.payload?.status === "server_info"),
      "legacy application admission",
    );
    await channel.send(
      JSON.stringify({
        type: "session",
        message: { type: "ping", requestId: "legacy-ping", clientSentAt: Date.now() },
      }),
    );
    await until(
      () =>
        messages.some(
          (m) =>
            m.type === "session" &&
            m.message?.type === "pong" &&
            m.message.payload?.requestId === "legacy-ping",
        ),
      "legacy encrypted RPC",
    );
  } finally {
    channel?.close();
    ws.close();
  }
}
async function relayGauges() {
  const response = await fetch(`http://${endpoint}/metrics`);
  assert(response.ok);
  const text = await response.text();
  return Object.fromEntries(
    [...text.matchAll(/^paseo_relay_(\w+) (\d+)$/gm)].map((match) => [match[1], Number(match[2])]),
  );
}
async function startDaemon(index, online = true) {
  const root = path.join(work, `daemon-${index}`);
  const home = path.join(root, "home");
  const cwd = path.join(root, "workspace");
  await mkdir(home, { recursive: true });
  await mkdir(cwd);
  const config = {
    listen: "127.0.0.1:0",
    paseoHome: home,
    staticDir: cwd,
    corsAllowedOrigins: [],
    hostnames: true,
    mcpEnabled: false,
    mcpDebug: false,
    browserToolsEnabled: false,
    pluginsEnabled: false,
    agentClients: {},
    worktreesRoot: path.join(root, "worktrees"),
    agentStoragePath: path.join(home, "agents"),
    relayEnabled: true,
    relayEndpoint: endpoint,
    relayUseTls: false,
    relayRequireProtectedSecurity: index === 0,
    auth: { password: hashSync(password, 4) },
    resumeOnStartup: false,
  };
  const daemon = await createPaseoDaemon(config, logger);
  const entry = { daemon, home, cwd, config };
  daemons.push(entry);
  if (online) await daemon.start();
  const pairing = await generateLocalPairingOffer({
    paseoHome: home,
    relayEnabled: true,
    relayEndpoint: endpoint,
    relayUseTls: false,
    includeQr: false,
  });
  entry.offer = parseConnectionOfferFromUrl(pairing.url);
  assert(entry.offer);
  return entry;
}
let failure;
try {
  await phase("daemon-start", async () => {
    await startDaemon(0);
    await startDaemon(1);
    return { daemons: daemons.length };
  });
  await phase("connect", async () => {
    await Promise.all(
      Array.from({ length: count }, (_, i) =>
        makeClient(daemons[Math.floor(i / 2) % daemons.length], i).client.connect(),
      ),
    );
    await Promise.all(
      clients.map(async (e) => {
        await e.client.ping({ timeoutMs: 10000 });
        assert.deepEqual(e.modes, ["protected"]);
      }),
    );
    return { connected: count, protected: count };
  });
  const healthy = clients.slice();
  await phase("client-connects-before-daemon", async () => {
    const offline = await startDaemon(2, false);
    const entry = makeClient(offline, 11001, { reconnect: false });
    const pending = entry.client.connect();
    void pending.catch(() => {});
    let pressure;
    try {
      const deadline = Date.now() + 5000;
      do {
        pressure = await relayGauges();
        if (pressure.ingress_reserved_bytes > 0 && pressure.backpressured_sources > 0) break;
        assert(Date.now() < deadline, "encrypted hello did not wait under a charged byte lease");
        await delay(20);
      } while (true);
      await offline.daemon.start();
      await pending;
      await entry.client.ping();
      assert.deepEqual(entry.modes, ["protected"]);
      return {
        chargedIngressBytes: pressure.ingress_reserved_bytes,
        backpressuredSources: pressure.backpressured_sources,
        recovered: true,
      };
    } finally {
      await entry.client.close();
      await pending.catch(() => {});
      await offline.daemon.stop();
      assert.equal(daemons.pop(), offline);
    }
  });
  await phase("credential-and-key-rejection", async () => {
    const bad = makeClient(daemons[0], 10000, {
      password: "not-the-test-password",
      reconnect: false,
    });
    await assert.rejects(
      bad.client.connect(),
      (error) => getDaemonAuthFailureReason(error) === "incorrect_password",
    );
    await bad.client.close();
    const wrongKey = makeClient(daemons[0], 10001, {
      key: exportPublicKey(generateKeyPair().publicKey),
      reconnect: false,
    });
    await assert.rejects(wrongKey.client.connect());
    assert.deepEqual(wrongKey.modes, []);
    await wrongKey.client.close();
    await Promise.all(healthy.map((e) => e.client.ping()));
    return { rejectedCredentials: 1, rejectedDaemonKey: 1, healthyClients: healthy.length };
  });
  await phase("legacy-compatibility-and-protected-policy", async () => {
    await legacyConnection(daemons[1], true);
    await legacyConnection(daemons[0], false);
    return { legacyAdmittedByOptionalPolicy: true, legacyRejectedByProtectedPolicy: true };
  });
  await phase("protected-record-replay-and-tamper", async () => {
    for (const attack of ["replay", "tamper"]) {
      const entry = makeClient(daemons[0], 30001, { reconnect: false });
      await entry.client.connect();
      const ws = entry.physical.at(-1);
      const original = ws.send.bind(ws);
      let captured;
      ws.send = (data) => {
        captured = data;
        if (attack === "replay") return original(data);
        ws.send = original;
        const bytes = typeof data === "string" ? Buffer.from(data, "base64") : Buffer.from(data);
        assert(bytes.length > 16);
        bytes[bytes.length - 1] ^= 1;
        return original(typeof data === "string" ? bytes.toString("base64") : bytes);
      };
      if (attack === "replay") {
        await entry.client.ping();
        assert(captured);
        ws.send(captured);
      } else {
        await assert.rejects(entry.client.ping({ timeoutMs: 10000 }));
      }
      await until(() => !entry.client.isConnected, `${attack} closed the secured session`);
      await entry.client.close();
    }
    await Promise.all(healthy.map((e) => e.client.ping()));
    return { replayRejected: true, corruptedTagRejected: true, healthyPeers: healthy.length };
  });
  await phase("encrypted-files-and-session-isolation", async () => {
    for (const d of daemons) {
      d.file = randomBytes(1024 * 1024 + 123);
      d.small = randomBytes(16384 + 17);
      await writeFile(path.join(d.cwd, "données-文件.bin"), d.file);
      await writeFile(path.join(d.cwd, "small.bin"), d.small);
    }
    const verifiedTransfers = [];
    await Promise.all(
      healthy.map(async (e, i) => {
        const file = await e.client.readFile(
          e.daemon.cwd,
          "données-文件.bin",
          undefined,
          undefined,
          30000,
        );
        assert.equal(hash(file.bytes), hash(e.daemon.file));
        const bytes = randomBytes(256 * 1024 + i);
        const upload = await e.client.uploadFile({
          fileName: `upload-${i}.bin`,
          mimeType: "application/octet-stream",
          bytes,
        });
        assert.equal(upload.error, null);
        assert(upload.file);
        assert(path.resolve(upload.file.path).startsWith(path.resolve(e.daemon.home) + path.sep));
        const uploadedHash = hash(await readFile(upload.file.path));
        assert.equal(uploadedHash, hash(bytes));
        verifiedTransfers.push({
          client: i,
          daemon: daemons.indexOf(e.daemon),
          download: {
            bytes: file.bytes.length,
            expectedSha256: hash(e.daemon.file),
            actualSha256: hash(file.bytes),
          },
          upload: { bytes: bytes.length, expectedSha256: hash(bytes), actualSha256: uploadedHash },
        });
      }),
    );
    assert(wire.binary > 0, "binary file/terminal frames never reached relay");
    return {
      verifiedDownloads: healthy.length,
      verifiedUploads: healthy.length,
      distinctDaemonFiles: daemons.length,
      verifiedTransfers,
    };
  });
  await phase("real-terminal-binary-output", async () => {
    for (const d of daemons) {
      const entry = healthy.find((e) => e.daemon === d);
      if (!entry) continue;
      const workspace = await entry.client.createWorkspace({
        source: { kind: "directory", path: d.cwd },
        title: "Relay stress workspace",
      });
      assert(workspace.workspace, workspace.error);
      const created = await entry.client.createTerminal(d.cwd, "relay-stress", undefined, {
        workspaceId: workspace.workspace.id,
        command: "/bin/cat",
        args: [],
      });
      assert.equal(created.error, null);
      assert(created.terminal);
      const id = created.terminal.id;
      let text = "";
      const observation = entry.client.observeTerminal(id, (event) => {
        if (event.type === "output") text += Buffer.from(event.data).toString();
      });
      try {
        await observation.ready;
        const marker = `RELAY_TERMINAL_${randomBytes(12).toString("hex")}`;
        entry.client.sendTerminalInput(id, { type: "input", data: marker + "\n" });
        await until(() => text.includes(marker), "real PTY echoed encrypted input");
      } finally {
        await observation.release();
        await entry.client.killTerminal(id);
      }
    }
    return { terminals: daemons.filter((d) => healthy.some((e) => e.daemon === d)).length };
  });
  await phase("concurrent-mixed-rpc-load", async () => {
    const deadline = performance.now() + seconds * 1000;
    const latencies = [];
    let pings = 0,
      lists = 0,
      files = 0;
    await Promise.all(
      healthy.map(async (e, index) => {
        let sequence = 0;
        do {
          const requestId = `load_${index}_${sequence}`;
          const start = performance.now();
          const response = await e.client.ping({ requestId, timeoutMs: 10000 });
          assert.equal(response.requestId, requestId);
          pings++;
          latencies.push(performance.now() - start);
          const agents = await e.client.fetchAgents();
          assert.deepEqual(agents.entries, []);
          lists++;
          if (sequence % 5 === 0) {
            const result = await e.client.readFile(
              e.daemon.cwd,
              "small.bin",
              undefined,
              undefined,
              30000,
            );
            assert.equal(hash(result.bytes), hash(e.daemon.small));
            files++;
          }
          sequence++;
        } while (performance.now() < deadline);
      }),
    );
    latencies.sort((a, b) => a - b);
    const percentile = (p) =>
      latencies[Math.min(latencies.length - 1, Math.ceil(latencies.length * p) - 1)];
    return {
      clients: healthy.length,
      pings,
      agentLists: lists,
      verifiedFiles: files,
      pingMs: {
        p50: percentile(0.5),
        p95: percentile(0.95),
        p99: percentile(0.99),
        max: latencies.at(-1),
      },
    };
  });
  await phase("idle-with-production-heartbeats", async () => {
    const opens = healthy.map((e) => e.physical.length);
    const frames = wire.frames;
    await delay(12000);
    assert(wire.frames > frames, "production SDK heartbeat did not reach relay");
    assert(
      healthy.every((e, i) => e.client.isConnected && e.physical.length === opens[i]),
      "idle session reconnected instead of staying alive",
    );
    await Promise.all(healthy.map((e) => e.client.ping()));
    return { idleSeconds: 12, newWireFrames: wire.frames - frames, unintendedReconnects: 0 };
  });
  if (process.env.PASEO_NETEM === "1") {
    await phase("network-delay-jitter-and-loss", async () => {
      const tc = (args) =>
        execFileSync(
          "docker",
          [
            "run",
            "--rm",
            "--network",
            `container:${process.env.PASEO_RELAY_CONTAINER}`,
            "--cap-add",
            "NET_ADMIN",
            "paseo-relay-netem:test",
            ...args,
          ],
          { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
        );
      const start = performance.now();
      let qdisc;
      try {
        tc([
          "qdisc",
          "replace",
          "dev",
          "eth0",
          "root",
          "netem",
          "delay",
          "100ms",
          "10ms",
          "loss",
          "0.5%",
        ]);
        await Promise.all(
          healthy.map(async (e) => {
            for (let n = 0; n < 3; n++) await e.client.ping({ timeoutMs: 15000 });
            const result = await e.client.readFile(
              e.daemon.cwd,
              "small.bin",
              undefined,
              undefined,
              30000,
            );
            assert.equal(hash(result.bytes), hash(e.daemon.small));
          }),
        );
        qdisc = tc(["-s", "qdisc", "show", "dev", "eth0"]);
        assert(qdisc.includes("netem") && qdisc.includes("100ms"));
      } finally {
        tc(["qdisc", "del", "dev", "eth0", "root"]);
      }
      return {
        simulatedNetwork: true,
        egressDelayMs: 100,
        jitterMs: 10,
        lossPercent: 0.5,
        secondsUnderImpairment: (performance.now() - start) / 1000,
        qdisc,
      };
    });
  }
  await phase("slow-mobile-reader-isolation", async () => {
    const slow = healthy.find((e) => !e.browser);
    assert(slow);
    const bytes = randomBytes(8 * 1024 * 1024 + 77);
    await writeFile(path.join(slow.daemon.cwd, "slow.bin"), bytes);
    const ws = slow.physical.at(-1);
    assert(ws._socket);
    let completed = false;
    ws._socket.pause();
    const transfer = slow.client
      .readFile(slow.daemon.cwd, "slow.bin", undefined, undefined, 30000)
      .then((value) => {
        completed = true;
        return value;
      });
    void transfer.catch(() => {});
    let result, pressure;
    try {
      await delay(500);
      assert(!completed, "paused reader unexpectedly finished");
      const metrics = await relayGauges();
      pressure = {
        ingressBytes: metrics.ingress_reserved_bytes,
        backpressuredSources: metrics.backpressured_sources,
      };
      await Promise.all(
        healthy.filter((e) => e !== slow).map((e) => e.client.ping({ timeoutMs: 10000 })),
      );
    } finally {
      ws._socket.resume();
      result = await transfer;
    }
    assert.equal(hash(result.bytes), hash(bytes));
    return {
      pressure,
      pausedMs: 500,
      healthyPeers: healthy.length - 1,
      verifiedBytes: bytes.length,
      expectedSha256: hash(bytes),
      actualSha256: hash(result.bytes),
    };
  });
  await phase("client-data-socket-reconnect", async () => {
    const entry = healthy.find((e) => !e.browser);
    const opens = entry.physical.length;
    entry.physical.at(-1).terminate();
    await until(
      () => entry.physical.length > opens && entry.client.isConnected,
      "client data socket reconnect",
    );
    await entry.client.ping({ timeoutMs: 10000 });
    assert(entry.modes.every((m) => m === "protected"));
    return { opensBefore: opens, opensAfter: entry.physical.length };
  });
  await phase("connection-churn", async () => {
    const batch = Math.min(count, 16);
    for (let round = 0; round < 3; round++) {
      const transient = Array.from({ length: batch }, (_, i) =>
        makeClient(daemons[i % daemons.length], 20000 + round * batch + i, { reconnect: false }),
      );
      try {
        await Promise.all(
          transient.map(async (e) => {
            await e.client.connect();
            await e.client.ping();
          }),
        );
      } finally {
        await Promise.all(transient.map((e) => e.client.close()));
      }
    }
    await Promise.all(healthy.map((e) => e.client.ping()));
    return { rounds: 3, clientsPerRound: batch };
  });
  await phase("relay-restart-auto-recovery", async () => {
    const previous = healthy.map((e) => e.physical.length);
    execFileSync("docker", ["restart", "--timeout", "1", process.env.PASEO_RELAY_CONTAINER], {
      stdio: "pipe",
    });
    await until(
      () => healthy.every((e, i) => e.physical.length > previous[i] && e.client.isConnected),
      "daemon control and SDK reconnect after relay restart",
      60000,
    );
    await Promise.all(
      healthy.map(async (e) => {
        await e.client.ping({ timeoutMs: 10000 });
        const result = await e.client.readFile(
          e.daemon.cwd,
          "small.bin",
          undefined,
          undefined,
          30000,
        );
        assert.equal(hash(result.bytes), hash(e.daemon.small));
        assert(e.modes.length >= 2 && e.modes.every((m) => m === "protected"));
      }),
    );
    return { recovered: healthy.length, daemonControlReconnects: daemons.length };
  });
  await phase("daemon-restart-auto-recovery", async () => {
    const d = daemons[0];
    const affected = healthy.filter((e) => e.daemon === d);
    const previous = affected.map((e) => e.physical.length);
    await d.daemon.stop();
    d.daemon = await createPaseoDaemon(d.config, logger);
    await d.daemon.start();
    assert.equal(d.daemon.getServerId(), d.offer.serverId);
    await until(
      () => affected.every((e, i) => e.physical.length > previous[i] && e.client.isConnected),
      "daemon reconnect with retained identity",
      60000,
    );
    await Promise.all(
      healthy.map(async (e) => {
        await e.client.ping({ timeoutMs: 10000 });
        const file = await e.client.readFile(
          e.daemon.cwd,
          "small.bin",
          undefined,
          undefined,
          30000,
        );
        assert.equal(hash(file.bytes), hash(e.daemon.small));
        assert(e.modes.every((m) => m === "protected"));
      }),
    );
    return {
      recovered: affected.length,
      unaffectedPeers: healthy.length - affected.length,
      identityRetained: true,
    };
  });
  assert(wire.hello >= count && wire.ready >= count && wire.binary > 0);
  assert.equal(wire.plaintextLeaks, 0, "daemon credentials appeared on physical relay wire");
} catch (error) {
  failure = error;
  emit({ status: "failed", error: error.stack });
} finally {
  const cleanup = [
    ...(await Promise.allSettled(clients.map((e) => e.client.close()))),
    ...(await Promise.allSettled(daemons.map((e) => e.daemon.stop()))),
  ];
  try {
    assert(
      cleanup.every((result) => result.status === "fulfilled"),
      "client/daemon cleanup rejected",
    );
    await until(() => sockets.size === 0, "all physical client sockets closed", 5000);
  } catch (error) {
    failure ??= error;
    emit({ status: "failed", error: error.stack });
  }
  loopDelay.disable();
  const modules = [];
  for (const url of imported) {
    if (!url.startsWith("file:")) continue;
    const p = fileURLToPath(url);
    if (!p.startsWith(source + "/packages/")) continue;
    modules.push({ path: path.relative(source, p), sha256: hash(await readFile(p)) });
  }
  const required = [
    "packages/client/src/daemon-client.ts",
    "packages/client/src/daemon-client-relay-e2ee-transport.ts",
    "packages/server/src/server/relay-transport.ts",
    "packages/server/src/server/websocket/secure-relay-ingress.ts",
    "packages/relay/src/secure-channel.ts",
    "packages/protocol/src/messages.ts",
  ];
  const seen = new Set(modules.map((m) => m.path));
  const missing = required.filter((p) => !seen.has(p));
  const stale = modules.filter((m) => /^packages\/(client|protocol|relay)\/dist\//.test(m.path));
  if (missing.length || stale.length) {
    failure ??= new Error(
      `source verification failed: missing ${missing.join(",")}; stale dist imports ${stale.map((m) => m.path).join(",")}`,
    );
    emit({ status: "failed", error: failure.message });
  }
  await writeFile(
    path.join(output, "client-results.json"),
    JSON.stringify(
      {
        passed: !failure,
        results,
        wire,
        modules,
        sourceVerification: { missing, stale },
        cleanup: {
          rejected: cleanup.filter((result) => result.status === "rejected").length,
          openClientSockets: sockets.size,
        },
        driver: {
          cpu: process.cpuUsage(cpuStart),
          maxRssKiB: process.resourceUsage().maxRSS,
          eventLoopDelayP99Ms: loopDelay.percentile(99) / 1e6,
        },
      },
      null,
      2,
    ) + "\n",
  );
}
process.exit(failure ? 1 : 0);
