import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { runWithFailedTrailer } from "./lib/failed-trailer.mjs";
import { terminateManagedChild } from "./lib/managed-child-process.mjs";
import { createPnpmRunnerSpawnSpec } from "./pnpm-runner.mjs";

const KNIP_VERSION = "6.8.0";
const KNIP_TIMEOUT_MS = 10 * 60 * 1000;
const KNIP_KILL_GRACE_MS = 5_000;
const KNIP_PROCESS_TREE_EXIT_POLL_MS = 25;
const KNIP_POST_FORCE_KILL_WAIT_MS = 1_000;
const KNIP_HEARTBEAT_MS = 60_000;
const PNPM_DLX_LAYOUT_ENV_KEYS = new Set([
  "pnpm_config_modules_dir",
  "pnpm_config_virtual_store_dir",
  "npm_config_modules_dir",
  "npm_config_virtual_store_dir",
]);

/** Maximum buffered Knip output retained for diagnostics. */
export const KNIP_MAX_BUFFER_BYTES = 16 * 1024 * 1024;

function normalizeRepoPath(value) {
  return value.replaceAll("\\", "/").replace(/^\.\//u, "");
}

export function uniqueSorted(values) {
  return [...new Set(values.map(normalizeRepoPath))].toSorted((left, right) =>
    left.localeCompare(right),
  );
}

export function isLikelyRepoFilePath(value) {
  const normalized = normalizeRepoPath(value);
  return (
    !normalized.startsWith("/") &&
    !/^[A-Za-z]:/u.test(normalized) &&
    !normalized.startsWith("../") &&
    !normalized.includes("/../") &&
    /\.(?:[cm]?[jt]sx?)$/u.test(normalized)
  );
}

function spawnErrorCode(error) {
  return error && typeof error === "object" && "code" in error ? String(error.code) : undefined;
}

function createKnipChildEnv(env) {
  const childEnv = { ...(env ?? process.env) };
  for (const key of Object.keys(childEnv)) {
    if (PNPM_DLX_LAYOUT_ENV_KEYS.has(key.toLowerCase())) {
      delete childEnv[key];
    }
  }
  return childEnv;
}

function processTreeAlive(child, platform) {
  if (platform === "win32" || !child.pid) {
    return false;
  }
  try {
    process.kill(-child.pid, 0);
    return true;
  } catch (error) {
    return error?.code === "EPERM";
  }
}

async function waitForProcessTreeExit(child, platform, timeoutMs) {
  const deadlineAt = Date.now() + timeoutMs;
  while (Date.now() < deadlineAt) {
    if (!processTreeAlive(child, platform)) {
      return true;
    }
    await new Promise((resolvePoll) => {
      setTimeout(resolvePoll, KNIP_PROCESS_TREE_EXIT_POLL_MS);
    });
  }
  return !processTreeAlive(child, platform);
}

function withProcessTreeCleanupFailure(result, platform) {
  const platformName = platform === "win32" ? "Windows " : "";
  return {
    ...result,
    errorCode: "EPROCESSGROUP_CLEANUP_FAILED",
    errorMessage: `${result.errorMessage}; ${platformName}process tree cleanup could not be verified`,
  };
}

/** Runs pinned Knip with the supplied CLI arguments. */
export async function runKnip(knipArgs, params = {}) {
  const run = params.spawnCommand ?? spawn;
  const timeoutMs = params.timeoutMs ?? KNIP_TIMEOUT_MS;
  const heartbeatMs = params.heartbeatMs ?? KNIP_HEARTBEAT_MS;
  const maxBufferBytes = params.maxBufferBytes ?? KNIP_MAX_BUFFER_BYTES;
  const killGraceMs = params.killGraceMs ?? KNIP_KILL_GRACE_MS;
  const scanName = params.scanName ?? "scan";
  const writeStatus = params.writeStatus ?? ((message) => process.stderr.write(`${message}\n`));
  const platform = params.platform ?? process.platform;
  const runTaskkill = params.runTaskkill;
  const args = [
    "--config.minimum-release-age=0",
    "dlx",
    "--package",
    `knip@${KNIP_VERSION}`,
    "knip",
    ...knipArgs,
  ];

  return await new Promise((resolve) => {
    const startedAt = Date.now();
    let settled = false;
    let timedOut = false;
    let bufferExceeded = false;
    let outputBytes = 0;
    const output = [];
    let killTimer;
    let exitStatus = null;
    let exitSignal = null;

    const pnpm = createPnpmRunnerSpawnSpec({
      detached: platform !== "win32",
      env: createKnipChildEnv(params.env),
      nodeExecPath: params.nodeExecPath,
      npmExecPath: params.npmExecPath,
      platform,
      pnpmArgs: args,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const child = run(pnpm.command, pnpm.args, {
      ...pnpm.options,
      detached: platform !== "win32",
      stdio: ["ignore", "pipe", "pipe"],
    });
    const parentSignalHandlers = [];
    const cleanupParentSignalHandlers = () => {
      for (const { signal, handler } of parentSignalHandlers) {
        process.off(signal, handler);
      }
      parentSignalHandlers.length = 0;
    };
    const relayParentSignal = (signal) => {
      const handler = () => {
        terminateManagedChild(child, signal, { platform, runTaskkill });
        if (platform !== "win32") {
          terminateManagedChild(child, "SIGKILL", { platform });
        }
        cleanupParentSignalHandlers();
        process.kill(process.pid, signal);
      };
      parentSignalHandlers.push({ signal, handler });
      process.once(signal, handler);
    };
    if (process.platform !== "win32") {
      relayParentSignal("SIGINT");
      relayParentSignal("SIGTERM");
      relayParentSignal("SIGHUP");
    }

    const heartbeatTimer = setInterval(() => {
      writeStatus(
        `[deadcode] Knip ${scanName} still running after ${Math.round((Date.now() - startedAt) / 1000)}s.`,
      );
    }, heartbeatMs);
    const finish = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeoutTimer);
      clearInterval(heartbeatTimer);
      clearTimeout(killTimer);
      cleanupParentSignalHandlers();
      resolve({ ...result, output: output.join("") });
    };
    const finishAfterProcessTreeCleanup = async (result) => {
      if (settled) {
        return;
      }
      if (processTreeAlive(child, platform)) {
        await waitForProcessTreeExit(child, platform, killGraceMs);
      }
      if (processTreeAlive(child, platform)) {
        terminateManagedChild(child, "SIGKILL", { platform });
        await waitForProcessTreeExit(child, platform, KNIP_POST_FORCE_KILL_WAIT_MS);
      }
      if (processTreeAlive(child, platform)) {
        finish(withProcessTreeCleanupFailure(result, platform));
        return;
      }
      finish(result);
    };
    const terminateChild = (signal, failureResult) => {
      const termination = terminateManagedChild(child, signal, {
        platform,
        runTaskkill,
      });
      if (termination?.processTreeState !== "indeterminate") {
        return true;
      }
      child.stdout?.destroy?.();
      child.stderr?.destroy?.();
      child.unref?.();
      finish(withProcessTreeCleanupFailure(failureResult, platform));
      return false;
    };
    const scheduleForceKill = (failureResult) => {
      if (platform === "win32") {
        return;
      }
      killTimer = setTimeout(() => {
        terminateChild("SIGKILL", failureResult);
      }, killGraceMs);
    };

    const appendOutput = (chunk) => {
      if (settled || bufferExceeded) {
        return;
      }
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
      const remainingBytes = maxBufferBytes - outputBytes;
      if (buffer.length <= remainingBytes) {
        output.push(buffer.toString("utf8"));
        outputBytes += buffer.length;
        return;
      }
      if (remainingBytes > 0) {
        output.push(buffer.subarray(0, remainingBytes).toString("utf8"));
        outputBytes = maxBufferBytes;
      }
      bufferExceeded = true;
      writeStatus(
        `[deadcode] Knip ${scanName} exceeded ${maxBufferBytes} output bytes; terminating.`,
      );
      child.stdout?.off?.("data", appendOutput);
      child.stderr?.off?.("data", appendOutput);
      child.stdout?.destroy?.();
      child.stderr?.destroy?.();
      clearInterval(heartbeatTimer);
      const failureResult = {
        errorCode: "ENOBUFS",
        errorMessage: `Knip ${scanName} exceeded ${maxBufferBytes} output bytes`,
        signal: exitSignal,
        status: exitStatus,
      };
      if (terminateChild("SIGTERM", failureResult)) {
        scheduleForceKill(failureResult);
      }
    };

    child.stdout?.on("data", appendOutput);
    child.stderr?.on("data", appendOutput);
    const timeoutTimer = setTimeout(() => {
      timedOut = true;
      clearInterval(heartbeatTimer);
      writeStatus(
        `[deadcode] Knip ${scanName} timed out after ${Math.round(timeoutMs / 1000)}s; terminating.`,
      );
      const failureResult = {
        errorCode: "ETIMEDOUT",
        errorMessage: `Knip ${scanName} timed out after ${Math.round(
          (Date.now() - startedAt) / 1000,
        )}s`,
        signal: exitSignal,
        status: exitStatus,
      };
      if (terminateChild("SIGTERM", failureResult)) {
        scheduleForceKill(failureResult);
      }
    }, timeoutMs);
    child.on("error", (error) =>
      finish({
        errorCode: spawnErrorCode(error),
        errorMessage: error.message,
        signal: null,
        status: null,
      }),
    );
    child.on("exit", (status, signal) => {
      exitStatus = status;
      exitSignal = signal;
    });
    child.on("close", (status, signal) => {
      exitStatus = exitStatus ?? status;
      exitSignal = exitSignal ?? signal;
      const elapsedSeconds = Math.round((Date.now() - startedAt) / 1000);
      if (timedOut) {
        void finishAfterProcessTreeCleanup({
          errorCode: "ETIMEDOUT",
          errorMessage: `Knip ${scanName} timed out after ${elapsedSeconds}s`,
          signal: exitSignal,
          status: exitStatus,
        });
        return;
      }
      if (bufferExceeded) {
        void finishAfterProcessTreeCleanup({
          errorCode: "ENOBUFS",
          errorMessage: `Knip ${scanName} exceeded ${maxBufferBytes} output bytes`,
          signal: exitSignal,
          status: exitStatus,
        });
        return;
      }
      finish({
        errorCode: undefined,
        errorMessage: undefined,
        signal: exitSignal,
        status: exitStatus,
      });
    });
  });
}

async function main() {
  const result = await runKnip(process.argv.slice(2), { scanName: "command" });
  if (result.output) {
    process.stdout.write(result.output);
  }
  const exitCode = result.errorCode === undefined ? (result.status ?? 1) : 1;
  if (result.errorMessage) {
    process.stderr.write(`[deadcode] ${result.errorMessage}\n`);
  }
  process.exitCode = exitCode;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await runWithFailedTrailer("deadcode", main);
}
