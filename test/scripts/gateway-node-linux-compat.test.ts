import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import {
  chmodSync,
  closeSync,
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";
import { WebSocket, WebSocketServer } from "ws";
import {
  canonicalizeGatewayNodeCompatEvidence,
  validateGatewayNodeCompatEvidence,
  type GatewayNodeCompatOperation,
} from "../../scripts/gateway-node-compat-evidence.mjs";
import {
  GATEWAY_NODE_COMPAT_BASELINE_SPEC,
  resolvePackDestinationTarball,
} from "../../scripts/lib/cross-os-release-checks/config.ts";
import {
  assertGatewayNodeCompatRuntimeTreeReadOnly,
  assertGatewayNodeCompatArtifactSafe,
  buildDisjointPackagedClientScript,
  buildGatewayNodeCompatChildEnv,
  buildGatewayNodeCompatCases,
  buildGatewayNodeCompatDockerArgs,
  buildGatewayNodeCompatPreparationDockerArgs,
  buildGatewayNodeCompatEvidence,
  buildGatewayNodeCompatExecutionPlan,
  buildGatewayNodeCompatGatewayArgs,
  buildGatewayNodeCompatInvokeArgs,
  buildGatewayNodeCompatNodeArgs,
  fetchGatewayNodeCompatProducerJobs,
  GATEWAY_NODE_COMPAT_CONTAINER_IMAGE,
  normalizeProtocolMismatch,
  parseGatewayNodeCompatRunParams,
  readGatewayNodeCompatExecutionPlan,
  resolveGatewayNodeCompatCandidateIdentity,
  resolveGatewayNodeCompatArchitecture,
  resolveGatewayNodeCompatAcceptedMin,
  runGatewayNodeCompatDockerPhase,
  sealGatewayNodeCompatRuntimeTree,
  selectExpectedPendingNodeRequest,
  stageGatewayNodeCompatPackageFile,
  startProtocolObserver,
  validateGatewayNodeCompatArtifactBinding,
  validateGatewayNodeCompatContainerEvidence,
  validateGatewayNodeCompatObservation,
  validateGatewayNodeCompatPackageInputs,
  validateGatewayNodeCompatPreparedRuntime,
  withGatewayNodeCompatCleanup,
  type GatewayNodeCompatPackageSelection,
} from "../../scripts/lib/cross-os-release-checks/gateway-node-compat.ts";
import {
  binDirForPrefix,
  installTarballPackage,
  installedEntryPath,
  npmCommand,
  readInstalledMetadata,
} from "../../scripts/lib/cross-os-release-checks/install.ts";
import { resolveInstalledCliInvocation } from "../../scripts/lib/cross-os-release-checks/installed.ts";
import {
  canConnectToLoopbackPort,
  registerActiveChildProcessTree,
  runCommand,
  stopGateway,
} from "../../scripts/lib/cross-os-release-checks/process.ts";

const SOURCE_SHA = "a".repeat(40);
const HEAD_SHA = "b".repeat(40);
const ARTIFACT_SHA = "c".repeat(64);
const IDENTITY_SHA = "d".repeat(64);
const WORKFLOW_SHA = "e".repeat(40);
const STARTED_AT = "2026-08-06T12:00:00.000Z";
const COMPLETED_AT = "2026-08-06T12:00:01.000Z";
const PREPARE_CONTAINER_NAME = `openclaw-gateway-node-compat-prepare-123-${"a".repeat(16)}`;
const RUNTIME_CONTAINER_NAME = `openclaw-gateway-node-compat-runtime-123-${"b".repeat(16)}`;
const DOCKER_PHASE_TIMEOUT_MS = 30 * 60_000;
const REAL_DOCKER_TEST_TIMEOUT_MS = 75 * 60_000;
const FORBIDDEN_CONTAINER_ENV_KEYS = [
  "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
  "ACTIONS_RUNTIME_TOKEN",
  "GH_ENTERPRISE_TOKEN",
  "GH_TOKEN",
  "GITHUB_ACTIONS",
  "GITHUB_ENTERPRISE_TOKEN",
  "GITHUB_ENV",
  "GITHUB_OUTPUT",
  "GITHUB_PATH",
  "GITHUB_STEP_SUMMARY",
  "GITHUB_TOKEN",
  "HOMEBREW_GITHUB_API_TOKEN",
  "RUNNER_TEMP",
  "RUNNER_TOOL_CACHE",
] as const;

const producer = {
  repository: "openclaw/openclaw",
  workflowSha: WORKFLOW_SHA,
  runId: "789",
  runAttempt: 3,
  job: "gateway_node_linux_compat",
};

const actions = {
  apiUrl: "https://api.github.test",
  token: "github-token",
  repository: producer.repository,
  headSha: HEAD_SHA,
  headBranch: "main",
  event: "workflow_dispatch",
  consumerRunAttempt: 3,
  consumerRunId: producer.runId,
  workflowPath: ".github/workflows/openclaw-cross-os-release-checks-reusable.yml",
};

function selection(name = "candidate-456-3", runId = "456"): GatewayNodeCompatPackageSelection {
  return {
    tgzPath: "/tmp/openclaw.tgz",
    version: "2026.8.6",
    sourceSha: SOURCE_SHA,
    sha256: ARTIFACT_SHA,
    actionsArtifact: {
      id: 123,
      name,
      digest: `sha256:${ARTIFACT_SHA}`,
      runId,
      runAttempt: 3,
    },
  };
}

function artifactFixture(
  params: {
    artifactName?: string;
    jobName?: string;
    runAttempt?: number;
    runId?: number;
    workflowPath?: string;
  } = {},
) {
  const runAttempt = params.runAttempt ?? 3;
  const runId = params.runId ?? 456;
  const isCurrentAttempt =
    runId === Number(actions.consumerRunId) && runAttempt === actions.consumerRunAttempt;
  return {
    artifactMetadata: {
      id: 123,
      name: params.artifactName ?? "candidate-456-3",
      digest: `sha256:${ARTIFACT_SHA}`,
      expired: false,
      size_in_bytes: 1024,
      workflow_run: { id: runId, head_sha: HEAD_SHA },
    },
    workflowRun: {
      id: runId,
      run_attempt: runAttempt,
      head_sha: HEAD_SHA,
      head_branch: "main",
      event: "workflow_dispatch",
      path: params.workflowPath ?? actions.workflowPath,
      status: isCurrentAttempt ? "in_progress" : "completed",
      conclusion: isCurrentAttempt ? null : "failure",
      repository: { full_name: producer.repository },
      head_repository: { full_name: producer.repository },
    },
    workflowJobs: {
      total_count: 1,
      jobs: [
        {
          id: 900,
          name: params.jobName ?? "prepare",
          run_id: runId,
          run_attempt: runAttempt,
          head_sha: HEAD_SHA,
          status: "completed",
          conclusion: "success",
        },
      ],
    },
  };
}

function parseParams(
  params: {
    candidateAttempt?: string;
    baselineAttempt?: string;
    candidateRunId?: string;
    baselineRunId?: string;
    workflowRef?: string;
    candidateTgz?: string;
    baselineTgz?: string;
  } = {},
) {
  return parseGatewayNodeCompatRunParams(
    {
      "output-dir": "./output",
      "candidate-tgz": params.candidateTgz ?? "./candidate.tgz",
      "candidate-version": "2026.8.6",
      "candidate-source-sha": SOURCE_SHA,
      "candidate-sha256": ARTIFACT_SHA,
      "candidate-artifact-id": "10",
      "candidate-artifact-name": `openclaw-cross-os-release-checks-candidate-${
        params.candidateRunId ?? producer.runId
      }-${params.candidateAttempt ?? "3"}`,
      "candidate-artifact-digest": ARTIFACT_SHA,
      "candidate-artifact-run-id": params.candidateRunId ?? producer.runId,
      "candidate-artifact-run-attempt": params.candidateAttempt ?? "3",
      "compat-baseline-tgz": params.baselineTgz ?? "./baseline.tgz",
      "compat-baseline-version": "2026.5.7",
      "compat-baseline-source-sha": SOURCE_SHA,
      "compat-baseline-sha256": ARTIFACT_SHA,
      "compat-baseline-artifact-id": "11",
      "compat-baseline-artifact-digest": `sha256:${ARTIFACT_SHA}`,
      "compat-baseline-artifact-run-id": params.baselineRunId ?? producer.runId,
      "compat-baseline-artifact-run-attempt": params.baselineAttempt ?? "3",
    },
    {
      GATEWAY_NODE_COMPAT_WORKFLOW_SHA: WORKFLOW_SHA,
      GITHUB_API_URL: actions.apiUrl,
      GITHUB_EVENT_NAME: actions.event,
      GITHUB_HEAD_REF: "",
      GITHUB_JOB: producer.job,
      GITHUB_REF_NAME: actions.headBranch,
      GITHUB_REPOSITORY: producer.repository,
      GITHUB_RUN_ATTEMPT: "3",
      GITHUB_RUN_ID: producer.runId,
      GITHUB_SHA: actions.headSha,
      GITHUB_TOKEN: actions.token,
      GITHUB_WORKFLOW_REF:
        params.workflowRef ?? `${producer.repository}/${actions.workflowPath}@refs/heads/main`,
    },
  );
}

function createActionsFetcher(params: ReturnType<typeof parseParams>, requests: string[]) {
  return async (path: string) => {
    requests.push(path);
    const artifactId = Number(path.match(/^actions\/artifacts\/(10|11)$/u)?.[1]);
    if (artifactId) {
      const artifactSelection = artifactId === 10 ? params.candidate : params.baseline;
      return {
        id: artifactId,
        name: artifactSelection.actionsArtifact.name,
        digest: artifactSelection.actionsArtifact.digest,
        expired: false,
        size_in_bytes: artifactId === 10 ? 1024 : 2048,
        workflow_run: {
          id: Number(artifactSelection.actionsArtifact.runId),
          head_sha: HEAD_SHA,
        },
      };
    }
    const runMatch = path.match(/^actions\/runs\/([1-9][0-9]*)\/attempts\/([1-9][0-9]*)$/u);
    if (runMatch) {
      const runId = Number(runMatch[1]);
      const runAttempt = Number(runMatch[2]);
      const isCurrentAttempt =
        runId === Number(actions.consumerRunId) && runAttempt === actions.consumerRunAttempt;
      return {
        id: runId,
        run_attempt: runAttempt,
        head_sha: HEAD_SHA,
        head_branch: "main",
        event: "workflow_dispatch",
        path: actions.workflowPath,
        status: isCurrentAttempt ? "in_progress" : "completed",
        conclusion: isCurrentAttempt ? null : "failure",
        repository: { full_name: producer.repository },
        head_repository: { full_name: producer.repository },
      };
    }
    const jobsMatch = path.match(
      /^actions\/runs\/([1-9][0-9]*)\/attempts\/([1-9][0-9]*)\/jobs\?per_page=100&page=1$/u,
    );
    if (jobsMatch) {
      const runId = Number(jobsMatch[1]);
      const runAttempt = Number(jobsMatch[2]);
      return {
        total_count: 1,
        jobs: [
          {
            id: runId * 10 + runAttempt,
            name: "prepare",
            run_id: runId,
            run_attempt: runAttempt,
            head_sha: HEAD_SHA,
            status: "completed",
            conclusion: "success",
          },
        ],
      };
    }
    throw new Error(`Unexpected request: ${path}`);
  };
}

function runtimeBinding(version: string) {
  return {
    packagedArtifact: {
      version,
      sourceSha: SOURCE_SHA,
      name: `openclaw-${version}.tgz`,
      sha256: ARTIFACT_SHA,
      actionsArtifact: {
        id: 123,
        name: "candidate-456-3",
        digest: `sha256:${ARTIFACT_SHA}` as const,
        sizeBytes: 1024,
        runId: "456",
        runAttempt: 3,
      },
    },
    installedRuntime: {
      version,
      sourceSha: SOURCE_SHA,
      packageSha256: ARTIFACT_SHA,
      identitySha256: IDENTITY_SHA,
    },
  };
}

function protocolObservation(
  params: {
    architecture?: "arm64" | "x64";
    clientId?: string;
    clientMax?: number;
    clientMin?: number;
    helloProtocol?: number | null;
    mode?: string;
    platform?: string;
    protocolError?: unknown | null;
    role?: string;
  } = {},
) {
  return {
    clientMin: params.clientMin ?? 3,
    clientMax: params.clientMax ?? 4,
    helloProtocol: params.helloProtocol === undefined ? 4 : params.helloProtocol,
    identity: {
      architecture: params.architecture ?? resolveGatewayNodeCompatArchitecture(process.arch),
      clientId: params.clientId ?? "node-host",
      mode: params.mode ?? "node",
      platform: params.platform ?? "linux",
      role: params.role ?? "node",
    },
    protocolError: params.protocolError ?? null,
  };
}

function executionPlanFixture() {
  const params = parseParams();
  return buildGatewayNodeCompatExecutionPlan({
    candidate: {
      ...params.candidate,
      tarballSizeBytes: 512,
      actionsArtifact: { ...params.candidate.actionsArtifact, sizeBytes: 1024 },
    },
    baseline: {
      ...params.baseline,
      tarballSizeBytes: 768,
      actionsArtifact: { ...params.baseline.actionsArtifact, sizeBytes: 2048 },
    },
    producer: params.producer,
  });
}

function runtimeBindingFromPlan(input: ReturnType<typeof executionPlanFixture>["candidate"]) {
  return {
    packagedArtifact: {
      version: input.version,
      sourceSha: input.sourceSha,
      name: input.artifactFileName,
      sha256: input.sha256,
      actionsArtifact: input.actionsArtifact,
    },
    installedRuntime: {
      version: input.version,
      sourceSha: input.sourceSha,
      packageSha256: input.sha256,
      identitySha256: IDENTITY_SHA,
    },
  };
}

function writeContainerEvidenceFixture(root: string, plan = executionPlanFixture()) {
  chmodSync(root, 0o700);
  for (const compatCase of buildGatewayNodeCompatCases()) {
    const passed = compatCase.outcome === "passed";
    const evidence = buildGatewayNodeCompatEvidence({
      compatCase,
      gateway: runtimeBindingFromPlan(plan[compatCase.gateway]),
      node: runtimeBindingFromPlan(plan[compatCase.node]),
      gatewayAcceptedNodeMin: 3,
      producer: plan.producer,
      observation: passed
        ? protocolObservation()
        : protocolObservation({
            clientMin: 1,
            clientMax: 2,
            helloProtocol: null,
            protocolError: {
              details: {
                code: "PROTOCOL_MISMATCH",
                clientMinProtocol: 1,
                clientMaxProtocol: 2,
                expectedProtocol: 4,
              },
            },
          }),
      operation: passed
        ? {
            method: "node.invoke",
            command: "system.which",
            params: { bins: ["node"] },
            ok: true,
            result: { bins: { node: "/usr/bin/node" } },
          }
        : undefined,
      mismatch: passed
        ? undefined
        : {
            code: "PROTOCOL_MISMATCH",
            clientMinProtocol: 1,
            clientMaxProtocol: 2,
            expectedProtocol: 4,
          },
      startedAt: STARTED_AT,
      completedAt: COMPLETED_AT,
    });
    writeFileSync(
      join(root, `${compatCase.caseId}.json`),
      canonicalizeGatewayNodeCompatEvidence(evidence),
      { mode: 0o600 },
    );
  }
  return plan;
}

function sha256File(path: string) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function prependAfterShebang(source: string, prefix: string) {
  if (!source.startsWith("#!")) {
    return `${prefix}\n${source}`;
  }
  const newline = source.indexOf("\n");
  return `${source.slice(0, newline + 1)}${prefix}\n${source.slice(newline + 1)}`;
}

function readGatewayNodeCompatTestDiagnostics(diagnosticsDir: string) {
  if (!existsSync(diagnosticsDir)) {
    return "No Gateway/node compatibility diagnostics were retained.";
  }
  return readdirSync(diagnosticsDir)
    .filter((name) => name.endsWith(".log"))
    .toSorted()
    .map((name) => `--- ${name} ---\n${readFileSync(join(diagnosticsDir, name), "utf8")}`)
    .join("\n");
}

describe("Gateway/node Linux compatibility producer", () => {
  it("builds compatibility child environments from an explicit allowlist", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-child-env-"));
    try {
      const homeDir = join(root, "home");
      const prefixDir = join(root, "prefix");
      const stateDir = join(homeDir, ".openclaw");
      mkdirSync(stateDir, { recursive: true });
      mkdirSync(prefixDir, { recursive: true });
      const lane = {
        name: "child-env",
        rootDir: root,
        prefixDir,
        homeDir,
        stateDir,
        appDataDir: stateDir,
        gatewayPort: 0,
        phaseTimings: [],
      };
      const inheritedEnv = Object.fromEntries(
        FORBIDDEN_CONTAINER_ENV_KEYS.map((key) => [key, `sentinel-${key}`]),
      );
      inheritedEnv.PATH = process.env.PATH ?? "";
      inheritedEnv.LANG = "C.UTF-8";

      const params = parseParams();
      expect(params.actions.token).toBe(actions.token);
      const env = buildGatewayNodeCompatChildEnv(lane, "gateway-token", prefixDir, inheritedEnv);
      for (const key of FORBIDDEN_CONTAINER_ENV_KEYS) {
        expect(env[key]).toBeUndefined();
      }
      expect(env.OPENCLAW_GATEWAY_TOKEN).toBe("gateway-token");
      expect(env.LANG).toBe("C.UTF-8");

      const result = await runCommand(
        process.execPath,
        [
          "-e",
          `const keys = ${JSON.stringify(FORBIDDEN_CONTAINER_ENV_KEYS)};
process.stdout.write(JSON.stringify(Object.fromEntries(
  keys.map((key) => [key, process.env[key] ?? null]),
)));`,
        ],
        {
          cwd: homeDir,
          env,
          logPath: join(root, "child.log"),
        },
      );
      expect(JSON.parse(result.stdout)).toEqual(
        Object.fromEntries(FORBIDDEN_CONTAINER_ENV_KEYS.map((key) => [key, null])),
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("quarantines npm lifecycle command-channel and credential probes", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-npm-env-"));
    try {
      const packageDir = join(root, "package");
      const homeDir = join(root, "home");
      const prefixDir = join(root, "prefix");
      const stateDir = join(homeDir, ".openclaw");
      const capturePath = join(root, "lifecycle-env.json");
      mkdirSync(packageDir, { recursive: true });
      mkdirSync(stateDir, { recursive: true });
      mkdirSync(prefixDir, { recursive: true });
      writeFileSync(
        join(packageDir, "package.json"),
        JSON.stringify({
          name: "openclaw-gateway-node-env-probe",
          version: "1.0.0",
          scripts: { postinstall: "node postinstall.cjs" },
        }),
      );
      writeFileSync(
        join(packageDir, "postinstall.cjs"),
        `const { writeFileSync } = require("node:fs");
const keys = ${JSON.stringify(FORBIDDEN_CONTAINER_ENV_KEYS)};
for (const key of ["GITHUB_ENV", "GITHUB_OUTPUT", "GITHUB_PATH", "GITHUB_STEP_SUMMARY"]) {
  if (process.env[key]) require("node:fs").appendFileSync(process.env[key], "candidate-poison=1\\n");
}
process.stdout.write("::error::candidate-command-channel-probe\\n");
writeFileSync(${JSON.stringify(capturePath)}, JSON.stringify(
  Object.fromEntries(keys.map((key) => [key, process.env[key] ?? null])),
));`,
      );
      const lane = {
        name: "npm-env",
        rootDir: root,
        prefixDir,
        homeDir,
        stateDir,
        appDataDir: stateDir,
        gatewayPort: 0,
        phaseTimings: [],
      };
      const inheritedEnv = Object.fromEntries(
        FORBIDDEN_CONTAINER_ENV_KEYS.map((key) => [key, `sentinel-${key}`]),
      );
      inheritedEnv.PATH = process.env.PATH ?? "";
      const env = buildGatewayNodeCompatChildEnv(lane, "gateway-token", prefixDir, inheritedEnv);
      const lifecycle = await runCommand(process.execPath, [join(packageDir, "postinstall.cjs")], {
        cwd: lane.homeDir,
        env: { ...env, npm_lifecycle_event: "postinstall" },
        logPath: join(root, "install.log"),
      });
      expect(JSON.parse(readFileSync(capturePath, "utf8"))).toEqual(
        Object.fromEntries(FORBIDDEN_CONTAINER_ENV_KEYS.map((key) => [key, null])),
      );
      expect(lifecycle.stdout).toContain("::error::candidate-command-channel-probe");
      expect(readFileSync(join(root, "install.log"), "utf8")).toContain(
        "::error::candidate-command-channel-probe",
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("runs candidate code in a hardened tokenless Docker namespace", () => {
    const plan = executionPlanFixture();
    const prepareArgs = buildGatewayNodeCompatPreparationDockerArgs({
      containerName: PREPARE_CONTAINER_NAME,
      planPath: "/tmp/plan.json",
      candidateTgzPath: "/tmp/candidate.tgz",
      baselineTgzPath: "/tmp/baseline.tgz",
      diagnosticsDir: "/tmp/diagnostics",
      preparedDir: "/tmp/prepared",
      workflowRoot: "/tmp/workflow",
    });
    const args = buildGatewayNodeCompatDockerArgs({
      containerName: RUNTIME_CONTAINER_NAME,
      plan,
      planPath: "/tmp/plan.json",
      diagnosticsDir: "/tmp/diagnostics",
      preparedDir: "/tmp/prepared",
      outputDir: "/tmp/output",
      workflowRoot: "/tmp/workflow",
    });
    const command = args.join(" ");
    const prepareCommand = prepareArgs.join(" ");
    expect(args.slice(0, 2)).toEqual(["run", "--rm"]);
    expect(prepareArgs.slice(0, 2)).toEqual(["run", "--rm"]);
    expect(args.slice(2, 4)).toEqual(["--name", RUNTIME_CONTAINER_NAME]);
    expect(prepareArgs.slice(2, 4)).toEqual(["--name", PREPARE_CONTAINER_NAME]);
    expect(command).toContain("--read-only");
    expect(command).toContain("--network none");
    expect(prepareCommand).not.toContain("--network none");
    expect(args).not.toContain("--pid");
    expect(command).toContain("--cap-drop ALL");
    for (const capability of ["CHOWN", "DAC_OVERRIDE", "KILL", "SETGID", "SETUID"]) {
      expect(command).toContain(`--cap-add ${capability}`);
      expect(prepareCommand).toContain(`--cap-add ${capability}`);
    }
    expect(command).toContain("--security-opt no-new-privileges");
    expect(command).toContain("--pids-limit 512");
    expect(command).toContain("--cpus 4");
    expect(command).toContain("--memory 8g");
    expect(command).toContain("--memory-swap 8g");
    expect(prepareCommand).toContain("NODE_OPTIONS=--max-old-space-size=5120");
    expect(command).not.toContain("NODE_OPTIONS=");
    expect(command).toContain("--log-driver none");
    expect(command).toContain("/tmp:rw,exec,nosuid,nodev,size=8589934592");
    expect(command).toContain("type=bind,src=/tmp/workflow/scripts,dst=/workflow/scripts,readonly");
    expect(command).toContain(
      "type=bind,src=/tmp/workflow/packages/normalization-core,dst=/workflow/packages/normalization-core,readonly",
    );
    expect(command).not.toContain("type=bind,src=/tmp/workflow,dst=/workflow,readonly");
    expect(command).toContain("type=bind,src=/tmp/prepared,dst=/openclaw-prepared,readonly");
    expect(prepareCommand).toContain(
      "type=bind,src=/tmp/candidate.tgz,dst=/openclaw-candidate.tgz,readonly",
    );
    expect(prepareCommand).toContain(
      "type=bind,src=/tmp/baseline.tgz,dst=/openclaw-baseline.tgz,readonly",
    );
    expect(prepareCommand).toContain("type=bind,src=/tmp/prepared,dst=/openclaw-prepared");
    expect(command).toContain("type=bind,src=/tmp/output,dst=/openclaw-output");
    expect(command).not.toContain("type=bind,src=/tmp/output,dst=/openclaw-output,readonly");
    expect(command).not.toContain("/var/run/docker.sock");
    expect(command).not.toContain("--privileged");
    expect(command).not.toContain("--env-file");
    for (const key of FORBIDDEN_CONTAINER_ENV_KEYS) {
      expect(command).not.toContain(key);
    }
    expect(args).toContain(GATEWAY_NODE_COMPAT_CONTAINER_IMAGE);
    expect(args.slice(-4, -2)).toEqual(["node", "--input-type=module"]);
    expect(args.at(-2)).toBe("--eval");
    expect(args.at(-1)).toContain(
      'import("file:///workflow/scripts/lib/cross-os-release-checks/gateway-node-compat.ts")',
    );
    expect(args.at(-1)).toContain('readGatewayNodeCompatExecutionPlan("/openclaw-plan.json")');
    expect(prepareArgs.at(-1)).toContain("prepareGatewayNodeLinuxCompatContainer");
  });

  it("seals finalized runtime trees before identity hashing and case execution", () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-runtime-seal-"));
    const runtimeRoot = join(root, "runtime");
    const nestedDir = join(runtimeRoot, "dist");
    const executable = join(runtimeRoot, "openclaw.mjs");
    const dataFile = join(nestedDir, "identity.json");
    try {
      mkdirSync(nestedDir, { recursive: true, mode: 0o700 });
      writeFileSync(executable, "#!/usr/bin/env node\n", { mode: 0o700 });
      writeFileSync(dataFile, '{"version":"test"}\n', { mode: 0o600 });
      const identity = {
        uid: process.getuid?.() ?? 0,
        gid: process.getgid?.() ?? 0,
      };

      sealGatewayNodeCompatRuntimeTree(runtimeRoot, identity);
      expect(() => assertGatewayNodeCompatRuntimeTreeReadOnly(runtimeRoot, identity)).not.toThrow();
      expect(lstatSync(runtimeRoot).mode & 0o777).toBe(0o555);
      expect(lstatSync(nestedDir).mode & 0o777).toBe(0o555);
      expect(lstatSync(executable).mode & 0o777).toBe(0o555);
      expect(lstatSync(dataFile).mode & 0o777).toBe(0o444);
      if (identity.uid !== 0) {
        expect(() => writeFileSync(dataFile, "mutated\n")).toThrow();
      }
    } finally {
      if (existsSync(nestedDir)) {
        chmodSync(nestedDir, 0o700);
      }
      if (existsSync(runtimeRoot)) {
        chmodSync(runtimeRoot, 0o700);
      }
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("normalizes only the pinned v2026.5.7 legacy mismatch from its observed connect frame", () => {
    const observation = protocolObservation({
      clientMin: 1,
      clientMax: 2,
      helloProtocol: null,
      protocolError: { details: { expectedProtocol: 3 } },
    });
    expect(normalizeProtocolMismatch({ legacyGatewayVersion: "2026.5.7", observation })).toEqual({
      code: "PROTOCOL_MISMATCH",
      clientMinProtocol: 1,
      clientMaxProtocol: 2,
      expectedProtocol: 3,
    });
    expect(() =>
      normalizeProtocolMismatch({ legacyGatewayVersion: "2026.5.8", observation }),
    ).toThrow(/structured PROTOCOL_MISMATCH/u);
    expect(() =>
      normalizeProtocolMismatch({
        legacyGatewayVersion: "2026.5.7",
        observation: { ...observation, clientMin: 2 },
      }),
    ).toThrow(/structured PROTOCOL_MISMATCH/u);
    expect(() =>
      normalizeProtocolMismatch({
        legacyGatewayVersion: "2026.5.7",
        observation: { ...observation, helloProtocol: 3 },
      }),
    ).toThrow(/structured PROTOCOL_MISMATCH/u);
    expect(
      normalizeProtocolMismatch({
        observation: {
          ...observation,
          protocolError: {
            details: {
              details: {
                code: "PROTOCOL_MISMATCH",
                clientMinProtocol: 1,
                clientMaxProtocol: 2,
                expectedProtocol: 4,
              },
            },
          },
        },
      }),
    ).toEqual({
      code: "PROTOCOL_MISMATCH",
      clientMinProtocol: 1,
      clientMaxProtocol: 2,
      expectedProtocol: 4,
    });
  });

  it("removes a named container after a successful Docker command", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-docker-success-"));
    const calls: string[][] = [];
    const runCommandImpl: typeof runCommand = async (_command, args) => {
      calls.push(args);
      return { exitCode: 0, stdout: "", stderr: "" };
    };
    try {
      await expect(
        runGatewayNodeCompatDockerPhase(
          {
            phase: "runtime",
            args: ["run", "--name", RUNTIME_CONTAINER_NAME, "image"],
            containerName: RUNTIME_CONTAINER_NAME,
            logPath: join(root, "runtime.log"),
            outputDir: join(root, "evidence"),
            timeoutMs: 25,
          },
          runCommandImpl,
        ),
      ).resolves.toEqual({ exitCode: 0, stdout: "", stderr: "" });
      expect(calls).toEqual([
        ["run", "--name", RUNTIME_CONTAINER_NAME, "image"],
        ["rm", "--force", RUNTIME_CONTAINER_NAME],
        ["container", "ls", "--all", "--format", "{{.Names}}"],
      ]);
      expect(existsSync(join(root, "diagnostics", "runtime-host.log"))).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("retains diagnostics and removes a named container after an ordinary nonzero exit", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-docker-nonzero-"));
    const runCommandImpl: typeof runCommand = async (_command, args) => ({
      exitCode: args[0] === "run" ? 7 : 0,
      stdout: "",
      stderr: args[0] === "run" ? "candidate failed" : "",
    });
    try {
      await expect(
        runGatewayNodeCompatDockerPhase(
          {
            phase: "runtime",
            args: ["run", "--name", RUNTIME_CONTAINER_NAME, "image"],
            containerName: RUNTIME_CONTAINER_NAME,
            logPath: join(root, "runtime.log"),
            outputDir: join(root, "evidence"),
            timeoutMs: 25,
          },
          runCommandImpl,
        ),
      ).rejects.toThrow(/diagnostics were retained/u);
      expect(readFileSync(join(root, "diagnostics", "runtime-host.log"), "utf8")).toContain(
        "Docker exited with status 7",
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("retains redacted diagnostics and removes a named container after a rejected Docker command", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-docker-rejection-"));
    const calls: Array<{ args: string[]; timeoutMs: number | undefined }> = [];
    const runCommandImpl: typeof runCommand = async (_command, args, options) => {
      calls.push({ args, timeoutMs: options.timeoutMs });
      if (args[0] === "run") {
        writeFileSync(
          options.logPath,
          "authorization=github_pat_abcdefghijklmnopqrstuvwxyz123456\n",
        );
        throw new Error(
          `Command timed out token=github_pat_abcdefghijklmnopqrstuvwxyz123456 ${"x".repeat(
            128 * 1024,
          )}`,
        );
      }
      if (args[0] === "rm") {
        return { exitCode: 0, stdout: RUNTIME_CONTAINER_NAME, stderr: "" };
      }
      return { exitCode: 0, stdout: "", stderr: "" };
    };
    try {
      await expect(
        runGatewayNodeCompatDockerPhase(
          {
            phase: "runtime",
            args: ["run", "--name", RUNTIME_CONTAINER_NAME, "image"],
            containerName: RUNTIME_CONTAINER_NAME,
            logPath: join(root, "runtime.log"),
            outputDir: join(root, "evidence"),
            timeoutMs: 25,
          },
          runCommandImpl,
        ),
      ).rejects.toThrow(/diagnostics were retained/u);
      expect(calls).toEqual([
        { args: ["run", "--name", RUNTIME_CONTAINER_NAME, "image"], timeoutMs: 25 },
        {
          args: ["rm", "--force", RUNTIME_CONTAINER_NAME],
          timeoutMs: 30_000,
        },
        {
          args: ["container", "ls", "--all", "--format", "{{.Names}}"],
          timeoutMs: 30_000,
        },
      ]);
      const diagnostic = readFileSync(join(root, "diagnostics", "runtime-host.log"), "utf8");
      expect(diagnostic).toContain("exitCode=unavailable");
      expect(diagnostic).toContain("[REDACTED");
      expect(diagnostic).not.toContain("github_pat_abcdefghijklmnopqrstuvwxyz123456");
      expect(Buffer.byteLength(diagnostic)).toBeLessThanOrEqual(64 * 1024);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects stale or unowned container names before Docker launch", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-docker-name-"));
    let called = false;
    const runCommandImpl: typeof runCommand = async () => {
      called = true;
      return { exitCode: 0, stdout: "", stderr: "" };
    };
    try {
      await expect(
        runGatewayNodeCompatDockerPhase(
          {
            phase: "runtime",
            args: ["run", "--name", "shared-container", "image"],
            containerName: "shared-container",
            logPath: join(root, "runtime.log"),
            outputDir: join(root, "evidence"),
            timeoutMs: 25,
          },
          runCommandImpl,
        ),
      ).rejects.toThrow(/container name is invalid/u);
      expect(called).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("removes the daemon-owned container before external SIGTERM exits the runner", async () => {
    if (process.platform === "win32") {
      return;
    }
    const root = mkdtempSync(join(tmpdir(), "gateway-node-docker-signal-"));
    const binDir = join(root, "bin");
    const containerState = join(root, "container.state");
    const readyPath = join(root, "ready");
    const dockerPath = join(binDir, "docker");
    mkdirSync(binDir);
    writeFileSync(
      dockerPath,
      `#!/bin/sh
set -eu
case "$1" in
  run)
    : > "$OPENCLAW_FAKE_CONTAINER_STATE"
    : > "$OPENCLAW_FAKE_DOCKER_READY"
    trap 'exit 143' HUP INT TERM
    while :; do sleep 1; done
    ;;
  rm)
    rm -f "$OPENCLAW_FAKE_CONTAINER_STATE"
    ;;
  container)
    if [ -e "$OPENCLAW_FAKE_CONTAINER_STATE" ]; then
      printf '%s\\n' "${RUNTIME_CONTAINER_NAME}"
    fi
    ;;
  *)
    exit 2
    ;;
esac
`,
      { mode: 0o755 },
    );
    const moduleUrl = pathToFileURL(
      join(process.cwd(), "scripts/lib/cross-os-release-checks/gateway-node-compat.ts"),
    ).href;
    const runnerScript = `
import { runGatewayNodeCompatDockerPhase } from ${JSON.stringify(moduleUrl)};
await runGatewayNodeCompatDockerPhase({
  phase: "runtime",
  args: ["run", "--name", ${JSON.stringify(RUNTIME_CONTAINER_NAME)}, "image"],
  containerName: ${JSON.stringify(RUNTIME_CONTAINER_NAME)},
  logPath: ${JSON.stringify(join(root, "runtime.log"))},
  outputDir: ${JSON.stringify(join(root, "evidence"))},
  timeoutMs: 60_000,
});
`;
    const runner = spawn(
      process.execPath,
      ["--import", "tsx", "--input-type=module", "-e", runnerScript],
      {
        cwd: process.cwd(),
        env: {
          ...process.env,
          PATH: `${binDir}:${process.env.PATH ?? ""}`,
          OPENCLAW_CROSS_OS_PROCESS_TREE_KILL_AFTER_MS: "500",
          OPENCLAW_FAKE_CONTAINER_STATE: containerState,
          OPENCLAW_FAKE_DOCKER_READY: readyPath,
        },
        stdio: ["ignore", "ignore", "pipe"],
      },
    );
    try {
      const deadline = Date.now() + 10_000;
      while (!existsSync(readyPath) && Date.now() < deadline) {
        await new Promise<void>((resolvePromise) => {
          setTimeout(resolvePromise, 25);
        });
      }
      expect(existsSync(readyPath)).toBe(true);
      expect(existsSync(containerState)).toBe(true);
      runner.kill("SIGTERM");
      const [status, signal] = (await once(runner, "close")) as [
        number | null,
        NodeJS.Signals | null,
      ];
      expect({ status, signal }).toEqual({ status: 143, signal: null });
      expect(existsSync(containerState)).toBe(false);
      expect(readFileSync(join(root, "runtime-container-cleanup.log"), "utf8")).toContain(
        "signal cleanup inventory status=0",
      );
    } finally {
      if (runner.exitCode === null && runner.signalCode === null) {
        runner.kill("SIGKILL");
      }
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("fails cleanup when the daemon still owns the named container", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-docker-cleanup-"));
    const runCommandImpl: typeof runCommand = async (_command, args) => ({
      exitCode: 0,
      stdout: args[0] === "container" ? `${RUNTIME_CONTAINER_NAME}\nother-container\n` : "",
      stderr: "",
    });
    try {
      await expect(
        runGatewayNodeCompatDockerPhase(
          {
            phase: "runtime",
            args: ["run", "--name", RUNTIME_CONTAINER_NAME, "image"],
            containerName: RUNTIME_CONTAINER_NAME,
            logPath: join(root, "runtime.log"),
            outputDir: join(root, "evidence"),
            timeoutMs: 25,
          },
          runCommandImpl,
        ),
      ).rejects.toThrow(/diagnostics were retained/u);
      expect(readFileSync(join(root, "diagnostics", "runtime-host.log"), "utf8")).toContain(
        "still exists",
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("does not resolve ws for non-compat script modes", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-ws-loader-"));
    try {
      const loaderPath = join(root, "reject-ws-loader.mjs");
      const logPath = join(root, "resolve-matrix.log");
      writeFileSync(
        loaderPath,
        `export async function resolve(specifier, context, nextResolve) {
  if (specifier === "ws" || specifier === "ws/package.json") {
    throw new Error("ws resolved outside Gateway/node compatibility mode");
  }
  return nextResolve(specifier, context);
}
`,
      );
      const result = await runCommand(
        process.execPath,
        [
          "--loader",
          loaderPath,
          "scripts/openclaw-cross-os-release-checks.ts",
          "--resolve-matrix",
          "true",
        ],
        { logPath },
      );
      expect(JSON.parse(result.stdout)).toMatchObject({ include: expect.any(Array) });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it.runIf(process.env.OPENCLAW_GATEWAY_NODE_MALICIOUS_DOCKER === "1")(
    "rejects malicious lifecycle, ws, runtime, and evidence-forgery attempts in real Docker",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "gateway-node-malicious-docker-"));
      try {
        const packLog = join(root, "pack.log");
        const baselinePack = await runCommand(
          npmCommand(),
          [
            "pack",
            "--ignore-scripts",
            "--json",
            GATEWAY_NODE_COMPAT_BASELINE_SPEC,
            "--pack-destination",
            root,
          ],
          { logPath: packLog, timeoutMs: 5 * 60_000 },
        );
        const publishedFile = (JSON.parse(baselinePack.stdout) as Array<{ filename: string }>)[0]!
          .filename;
        const publishedTgzPath = resolvePackDestinationTarball(
          publishedFile,
          root,
          "baseline npm pack",
        ).path;
        const unpackDir = join(root, "unpack");
        mkdirSync(unpackDir, { recursive: true });
        await runCommand("tar", ["-xzf", publishedTgzPath, "-C", unpackDir], {
          logPath: join(root, "unpack.log"),
        });
        const packageRoot = join(unpackDir, "package");
        const packageJsonPath = join(packageRoot, "package.json");
        const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8")) as {
          version: string;
          files?: string[];
          scripts?: Record<string, string>;
          dependencies?: Record<string, string>;
          openclaw?: { commit?: string };
        };
        const sourceSha = "1".repeat(40);
        packageJson.openclaw = { ...packageJson.openclaw, commit: sourceSha };
        writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`);
        const baselinePackDir = join(root, "baseline-pack");
        mkdirSync(baselinePackDir);
        const repackedBaseline = await runCommand(
          npmCommand(),
          [
            "pack",
            "--ignore-scripts",
            "--json",
            packageRoot,
            "--pack-destination",
            baselinePackDir,
          ],
          { logPath: join(root, "baseline-pack.log"), timeoutMs: 5 * 60_000 },
        );
        const baselineFile = (
          JSON.parse(repackedBaseline.stdout) as Array<{ filename: string }>
        )[0]!.filename;
        const baselineTgzPath = resolvePackDestinationTarball(
          baselineFile,
          baselinePackDir,
          "hostile baseline npm pack",
        ).path;

        const candidateIdentity = resolveGatewayNodeCompatCandidateIdentity(
          process.getuid?.() ?? 0,
        );
        const identityAssertion = `
const { getgid, getuid } = require("node:process");
const { readFileSync } = require("node:fs");
const capEff = /^CapEff:\\s+([0-9a-f]+)$/mu.exec(readFileSync("/proc/self/status", "utf8"))?.[1];
if (getuid() !== ${candidateIdentity.uid} || getgid() !== ${candidateIdentity.gid} || !/^0+$/u.test(capEff ?? "")) {
  throw new Error("candidate process retained trusted harness identity or capabilities");
}
`.trim();
        const attackScript = `
const { appendFileSync, writeFileSync } = require("node:fs");
${identityAssertion}
for (const target of ["/openclaw-prepared/manifest.json", "/openclaw-output/forged.json"]) {
  try { writeFileSync(target, "{\\"forged\\":true}\\n"); } catch {}
}
appendFileSync(__filename + ".ran", "ran\\n");
`.trim();
        writeFileSync(join(packageRoot, "malicious-postinstall.cjs"), attackScript);
        const originalPostinstall = packageJson.scripts?.postinstall;
        packageJson.scripts = {
          ...packageJson.scripts,
          postinstall: [
            "node malicious-postinstall.cjs",
            ...(originalPostinstall ? [originalPostinstall] : []),
          ].join(" && "),
        };

        const cliPath = join(packageRoot, "openclaw.mjs");
        writeFileSync(
          cliPath,
          prependAfterShebang(
            readFileSync(cliPath, "utf8"),
            `{
  const { createRequire } = await import("node:module");
  const require = createRequire(import.meta.url);
  ${identityAssertion}
  if (!require("node:fs").existsSync(new URL("./malicious-postinstall.cjs.ran", import.meta.url))) {
    throw new Error("candidate postinstall did not run in the sealed lifecycle phase");
  }
  try { require("node:fs").writeFileSync("/openclaw-output/forged-runtime.json", "{}\\n"); } catch {}
}`,
          ),
        );

        const require = createRequire(import.meta.url);
        const trustedWsRoot = dirname(require.resolve("ws/package.json"));
        const maliciousWsRoot = join(packageRoot, "malicious-ws");
        cpSync(trustedWsRoot, maliciousWsRoot, { recursive: true });
        for (const fileName of ["index.js", "wrapper.mjs"]) {
          const wsPath = join(maliciousWsRoot, fileName);
          const attack =
            fileName === "wrapper.mjs"
              ? `import { writeFileSync as maliciousWriteFileSync } from "node:fs";
try { maliciousWriteFileSync("/openclaw-output/forged-ws.json", "{}\\n"); } catch {}`
              : `try { require("node:fs").writeFileSync("/openclaw-output/forged-ws.json", "{}\\n"); } catch {}`;
          writeFileSync(wsPath, `${attack}\n${readFileSync(wsPath, "utf8")}`);
        }
        packageJson.dependencies = {
          ...packageJson.dependencies,
          ws: "file:./malicious-ws",
        };
        packageJson.files = [...new Set([...(packageJson.files ?? []), "malicious-ws"])];
        writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`);

        const candidatePackDir = join(root, "candidate-pack");
        mkdirSync(candidatePackDir);
        const candidatePack = await runCommand(
          npmCommand(),
          [
            "pack",
            "--ignore-scripts",
            "--json",
            packageRoot,
            "--pack-destination",
            candidatePackDir,
          ],
          { logPath: join(root, "candidate-pack.log"), timeoutMs: 5 * 60_000 },
        );
        const candidateFile = (JSON.parse(candidatePack.stdout) as Array<{ filename: string }>)[0]!
          .filename;
        const candidateTgzPath = resolvePackDestinationTarball(
          candidateFile,
          candidatePackDir,
          "hostile candidate npm pack",
        ).path;
        const candidateSha = sha256File(candidateTgzPath);
        const baselineSha = sha256File(baselineTgzPath);
        const plan = buildGatewayNodeCompatExecutionPlan({
          candidate: {
            tgzPath: candidateTgzPath,
            version: packageJson.version,
            sourceSha: sourceSha as string,
            sha256: candidateSha,
            tarballSizeBytes: readFileSync(candidateTgzPath).byteLength,
            artifactFileName: candidateFile,
            actionsArtifact: {
              id: 10,
              name: "candidate-456-3",
              digest: `sha256:${candidateSha}`,
              runId: "456",
              runAttempt: 3,
              sizeBytes: readFileSync(candidateTgzPath).byteLength,
            },
          },
          baseline: {
            tgzPath: baselineTgzPath,
            version: packageJson.version,
            sourceSha: sourceSha as string,
            sha256: baselineSha,
            tarballSizeBytes: readFileSync(baselineTgzPath).byteLength,
            artifactFileName: baselineFile,
            actionsArtifact: {
              id: 11,
              name: "baseline-456-3",
              digest: `sha256:${baselineSha}`,
              runId: "456",
              runAttempt: 3,
              sizeBytes: readFileSync(baselineTgzPath).byteLength,
            },
          },
          producer,
        });
        const planPath = join(root, "plan.json");
        writeFileSync(planPath, `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
        const preparedDir = join(root, "prepared");
        const outputDir = join(root, "output");
        const diagnosticsDir = join(root, "diagnostics");
        mkdirSync(preparedDir, { mode: 0o700 });
        mkdirSync(outputDir, { mode: 0o700 });
        mkdirSync(diagnosticsDir, { mode: 0o700 });

        try {
          await runGatewayNodeCompatDockerPhase({
            phase: "prepare",
            args: buildGatewayNodeCompatPreparationDockerArgs({
              containerName: PREPARE_CONTAINER_NAME,
              planPath,
              candidateTgzPath,
              baselineTgzPath,
              diagnosticsDir,
              preparedDir,
              workflowRoot: process.cwd(),
            }),
            containerName: PREPARE_CONTAINER_NAME,
            logPath: join(root, "docker-prepare.log"),
            outputDir,
            timeoutMs: DOCKER_PHASE_TIMEOUT_MS,
          });
        } catch (error) {
          throw new Error(readGatewayNodeCompatTestDiagnostics(diagnosticsDir), { cause: error });
        }
        validateGatewayNodeCompatPreparedRuntime(preparedDir);
        expect(
          existsSync(
            join(
              dirname(installedEntryPath(join(preparedDir, "candidate"))),
              "malicious-postinstall.cjs.ran",
            ),
          ),
        ).toBe(false);

        try {
          await runGatewayNodeCompatDockerPhase({
            phase: "runtime",
            args: buildGatewayNodeCompatDockerArgs({
              containerName: RUNTIME_CONTAINER_NAME,
              plan,
              planPath,
              diagnosticsDir,
              preparedDir,
              outputDir,
              workflowRoot: process.cwd(),
            }),
            containerName: RUNTIME_CONTAINER_NAME,
            logPath: join(root, "docker-runtime.log"),
            outputDir,
            timeoutMs: DOCKER_PHASE_TIMEOUT_MS,
          });
        } catch (error) {
          throw new Error(readGatewayNodeCompatTestDiagnostics(diagnosticsDir), { cause: error });
        }
        expect([...validateGatewayNodeCompatContainerEvidence(outputDir, plan)]).toHaveLength(6);
        expect(readdirSync(outputDir).some((name) => name.startsWith("forged"))).toBe(false);

        const firstFile = `${buildGatewayNodeCompatCases()[0]!.caseId}.json`;
        for (const attack of ["symlink", "oversized", "forged"] as const) {
          const attackDir = join(root, `attack-${attack}`);
          cpSync(outputDir, attackDir, { recursive: true });
          const filePath = join(attackDir, firstFile);
          if (attack === "symlink") {
            rmSync(filePath);
            symlinkSync("/dev/null", filePath);
          } else if (attack === "oversized") {
            writeFileSync(filePath, Buffer.alloc(64 * 1024 + 1));
          } else {
            const evidence = JSON.parse(readFileSync(filePath, "utf8")) as {
              producer: { job: string };
            };
            evidence.producer.job = "forged";
            writeFileSync(filePath, canonicalizeGatewayNodeCompatEvidence(evidence));
          }
          expect(() => validateGatewayNodeCompatContainerEvidence(attackDir, plan)).toThrow();
        }
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
    REAL_DOCKER_TEST_TIMEOUT_MS,
  );

  it("requires a bounded canonical execution plan", () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-plan-"));
    try {
      const planPath = join(root, "plan.json");
      const plan = executionPlanFixture();
      writeFileSync(planPath, `${JSON.stringify(plan, null, 2)}\n`);
      expect(readGatewayNodeCompatExecutionPlan(planPath)).toEqual(plan);
      writeFileSync(planPath, JSON.stringify(plan));
      expect(() => readGatewayNodeCompatExecutionPlan(planPath)).toThrow(/canonical JSON/u);
      writeFileSync(planPath, Buffer.alloc(64 * 1024 + 1, "x"));
      expect(() => readGatewayNodeCompatExecutionPlan(planPath)).toThrow(/bounded regular file/u);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("stages a bounded package without cross-filesystem clone semantics", () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-package-stage-"));
    try {
      const sourcePath = join(root, "source.tgz");
      const stagedPath = join(root, "staged.tgz");
      const bytes = Buffer.from("portable package bytes");
      writeFileSync(sourcePath, bytes);
      const input = {
        ...executionPlanFixture().candidate,
        tgzPath: sourcePath,
        sha256: createHash("sha256").update(bytes).digest("hex"),
        tarballSizeBytes: bytes.byteLength,
      };

      stageGatewayNodeCompatPackageFile(input, stagedPath);

      expect(readFileSync(stagedPath)).toEqual(bytes);
      expect(lstatSync(stagedPath).mode & 0o777).toBe(0o400);
      rmSync(stagedPath);
      expect(() =>
        stageGatewayNodeCompatPackageFile(
          {
            ...input,
            tarballSizeBytes: bytes.byteLength + 1,
          },
          stagedPath,
        ),
      ).toThrow(/declared regular file size/u);
      expect(() =>
        stageGatewayNodeCompatPackageFile({ ...input, sha256: "f".repeat(64) }, stagedPath),
      ).toThrow(/SHA-256 mismatch/u);
      expect(existsSync(stagedPath)).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("accepts only canonical evidence bound to the trusted execution plan", () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-container-output-"));
    try {
      const plan = writeContainerEvidenceFixture(root);
      chmodSync(root, 0o700);
      const evidence = validateGatewayNodeCompatContainerEvidence(root, plan);
      expect([...evidence.keys()].toSorted()).toEqual(
        buildGatewayNodeCompatCases()
          .map((compatCase) => `${compatCase.caseId}.json`)
          .toSorted(),
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["symlink", "extra", "oversized", "noncanonical", "forged-producer"] as const)(
    "rejects %s container evidence",
    (attack) => {
      const root = mkdtempSync(join(tmpdir(), `gateway-node-container-${attack}-`));
      try {
        const plan = writeContainerEvidenceFixture(root);
        chmodSync(root, 0o700);
        const fileName = `${buildGatewayNodeCompatCases()[0]!.caseId}.json`;
        const filePath = join(root, fileName);
        if (attack === "symlink") {
          rmSync(filePath);
          symlinkSync("/dev/null", filePath);
        } else if (attack === "extra") {
          writeFileSync(join(root, "candidate.log"), "::error::poison\n");
        } else if (attack === "oversized") {
          writeFileSync(filePath, Buffer.alloc(64 * 1024 + 1, "x"));
        } else if (attack === "noncanonical") {
          writeFileSync(filePath, ` ${readFileSync(filePath, "utf8")}`);
        } else {
          const evidence = JSON.parse(readFileSync(filePath, "utf8")) as {
            producer: { job: string };
          };
          evidence.producer.job = "forged-producer";
          writeFileSync(filePath, canonicalizeGatewayNodeCompatEvidence(evidence));
        }
        expect(() => validateGatewayNodeCompatContainerEvidence(root, plan)).toThrow();
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it("defines four packaged default cases and disjoint proof for both Gateways", () => {
    const cases = buildGatewayNodeCompatCases();
    const architecture = resolveGatewayNodeCompatArchitecture(process.arch);
    expect(cases.map(({ caseId, outcome }) => ({ caseId, outcome }))).toEqual([
      { caseId: `linux-${architecture}-candidate-gateway-candidate-node`, outcome: "passed" },
      { caseId: `linux-${architecture}-candidate-gateway-baseline-node`, outcome: "passed" },
      { caseId: `linux-${architecture}-baseline-gateway-candidate-node`, outcome: "passed" },
      { caseId: `linux-${architecture}-baseline-gateway-baseline-node`, outcome: "passed" },
      {
        caseId: `linux-${architecture}-candidate-gateway-disjoint-node`,
        outcome: "protocol-mismatch",
      },
      {
        caseId: `linux-${architecture}-baseline-gateway-disjoint-node`,
        outcome: "protocol-mismatch",
      },
    ]);
    expect(cases.filter((entry) => entry.outcome === "passed")).toHaveLength(4);
    expect(cases.filter((entry) => entry.outcome === "protocol-mismatch")).toHaveLength(2);
    expect(buildGatewayNodeCompatCases("arm64").map((entry) => entry.caseId)).toEqual(
      buildGatewayNodeCompatCases("x64").map((entry) =>
        entry.caseId.replace("linux-x64-", "linux-arm64-"),
      ),
    );
    expect(resolveGatewayNodeCompatArchitecture("x64")).toBe("x64");
    expect(resolveGatewayNodeCompatArchitecture("arm64")).toBe("arm64");
    expect(() => resolveGatewayNodeCompatArchitecture("s390x")).toThrow(/x64 or arm64/u);
  });

  it("uses explicit unconfigured startup and keeps tokens out of every argv", () => {
    expect(buildGatewayNodeCompatGatewayArgs(18789)).toEqual([
      "gateway",
      "run",
      "--bind",
      "loopback",
      "--port",
      "18789",
      "--force",
      "--allow-unconfigured",
    ]);
    const nodeArgs = buildGatewayNodeCompatNodeArgs(18789, "linux-case");
    const invokeArgs = buildGatewayNodeCompatInvokeArgs({
      gatewayUrl: "ws://127.0.0.1:18789",
      nodeId: "node-id",
    });
    expect(nodeArgs.join(" ")).not.toMatch(/protocol|token/iu);
    expect(invokeArgs).toContain("system.which");
    expect(invokeArgs.join(" ")).not.toMatch(/token/iu);
  });

  it("reads mismatch auth from env and overrides only the synthetic client", () => {
    const script = buildDisjointPackagedClientScript({
      gatewayRuntimeUrl: "file:///tmp/openclaw/dist/plugin-sdk/gateway-runtime.js",
      gatewayUrl: "ws://127.0.0.1:18789",
    });
    expect(script).toContain("process.env.OPENCLAW_GATEWAY_TOKEN");
    expect(script).toContain("minProtocol: 1");
    expect(script).toContain("maxProtocol: 2");
    expect(script).not.toContain("secret-token");
  });

  it("parses caller provenance and artifact producer attempts independently", () => {
    const params = parseParams({ candidateAttempt: "5", candidateRunId: "788" });
    expect(params.candidate.actionsArtifact).toEqual({
      id: 10,
      name: "openclaw-cross-os-release-checks-candidate-788-5",
      digest: `sha256:${ARTIFACT_SHA}`,
      runId: "788",
      runAttempt: 5,
    });
    expect(params.baseline.actionsArtifact.name).toBe(
      "openclaw-gateway-node-compat-baseline-789-3",
    );
    expect(params.producer.workflowSha).toBe(WORKFLOW_SHA);
    expect(params.actions.headSha).toBe(HEAD_SHA);
    expect(params.actions.workflowPath).toBe(actions.workflowPath);
    expect(() => parseParams({ baselineRunId: "788" })).toThrow(/current workflow run/u);
    expect(() => parseParams({ candidateAttempt: "4" })).toThrow(/newer than the consumer/u);
    expect(() => parseParams({ baselineAttempt: "4" })).toThrow(/newer than the consumer/u);
  });

  it("parses direct and called workflow refs without hardcoding a caller", () => {
    expect(parseParams().actions.workflowPath).toBe(actions.workflowPath);
    expect(
      parseParams({
        workflowRef: `${producer.repository}/.github/workflows/openclaw-release-checks.yml@refs/tags/v2026.8.6`,
      }).actions.workflowPath,
    ).toBe(".github/workflows/openclaw-release-checks.yml");
    expect(() =>
      parseParams({ workflowRef: `other/repo/${actions.workflowPath}@refs/heads/main` }),
    ).toThrow(/caller workflow/u);
  });

  it("validates direct and called prepare producers against exact run provenance", () => {
    const fixture = artifactFixture();
    expect(
      validateGatewayNodeCompatArtifactBinding({
        selection: selection(),
        actions,
        ...fixture,
      }),
    ).toMatchObject({ actionsArtifact: { sizeBytes: 1024 } });

    const calledWorkflowPath = ".github/workflows/openclaw-release-checks.yml";
    const releasePackageName = "release-package-under-test-456-3";
    const releasePackageFixture = artifactFixture({
      artifactName: releasePackageName,
      jobName: "Prepare release package artifact",
      workflowPath: calledWorkflowPath,
    });
    expect(
      validateGatewayNodeCompatArtifactBinding({
        selection: selection(releasePackageName),
        actions: { ...actions, workflowPath: calledWorkflowPath },
        ...releasePackageFixture,
      }),
    ).toMatchObject({
      actionsArtifact: {
        id: 123,
        name: releasePackageName,
        digest: `sha256:${ARTIFACT_SHA}`,
        runId: "456",
        runAttempt: 3,
        sizeBytes: 1024,
      },
    });

    const calledFixture = artifactFixture({
      jobName: "cross_os_release_checks / prepare",
      workflowPath: calledWorkflowPath,
    });
    expect(
      validateGatewayNodeCompatArtifactBinding({
        selection: selection(),
        actions: { ...actions, workflowPath: calledWorkflowPath },
        ...calledFixture,
      }),
    ).toMatchObject({ actionsArtifact: { sizeBytes: 1024 } });

    calledFixture.workflowJobs.total_count = 2;
    calledFixture.workflowJobs.jobs.push({
      ...calledFixture.workflowJobs.jobs[0]!,
      id: 901,
      name: "other_call / prepare",
    });
    expect(() =>
      validateGatewayNodeCompatArtifactBinding({
        selection: selection(),
        actions: { ...actions, workflowPath: calledWorkflowPath },
        ...calledFixture,
      }),
    ).toThrow(/must be unique/u);

    const wrongSha = structuredClone(fixture);
    wrongSha.workflowRun.head_sha = "f".repeat(40);
    expect(() =>
      validateGatewayNodeCompatArtifactBinding({
        selection: selection(),
        actions,
        ...wrongSha,
      }),
    ).toThrow(/workflow run does not match/u);

    const failedJob = structuredClone(fixture);
    failedJob.workflowJobs.jobs[0]!.conclusion = "failure";
    expect(() =>
      validateGatewayNodeCompatArtifactBinding({
        selection: selection(),
        actions,
        ...failedJob,
      }),
    ).toThrow(/producer job did not complete successfully/u);
  });

  it("validates each producer attempt and deduplicates equal attempts", async () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-package-inputs-"));
    try {
      const candidateTgz = join(root, "candidate.tgz");
      const baselineTgz = join(root, "baseline.tgz");
      writeFileSync(candidateTgz, Buffer.alloc(321));
      writeFileSync(baselineTgz, Buffer.alloc(654));
      const distinct = parseParams({
        candidateRunId: "788",
        candidateAttempt: "3",
        baselineAttempt: "3",
        candidateTgz,
        baselineTgz,
      });
      const distinctRequests: string[] = [];
      const distinctResult = await validateGatewayNodeCompatPackageInputs(
        distinct,
        createActionsFetcher(distinct, distinctRequests),
      );
      expect(distinctResult.map((entry) => entry.tarballSizeBytes)).toEqual([321, 654]);
      expect(distinctResult.map((entry) => entry.actionsArtifact.sizeBytes)).toEqual([1024, 2048]);
      expect(distinctRequests.filter((path) => path.includes("/runs/788/attempts/3"))).toHaveLength(
        2,
      );
      expect(distinctRequests.filter((path) => path.includes("/runs/789/attempts/3"))).toHaveLength(
        2,
      );

      const shared = parseParams({
        candidateAttempt: "2",
        baselineAttempt: "2",
        candidateTgz,
        baselineTgz,
      });
      const sharedRequests: string[] = [];
      const result = await validateGatewayNodeCompatPackageInputs(
        shared,
        createActionsFetcher(shared, sharedRequests),
      );
      expect(result.map((entry) => entry.actionsArtifact.runAttempt)).toEqual([2, 2]);
      expect(sharedRequests.filter((path) => path.includes("/attempts/2"))).toHaveLength(2);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("combines all bounded job pages and rejects incomplete or duplicate inventories", async () => {
    const jobs = Array.from({ length: 150 }, (_, index) => ({ id: index + 1 }));
    await expect(
      fetchGatewayNodeCompatProducerJobs(async (page) => ({
        total_count: jobs.length,
        jobs: page === 1 ? jobs.slice(0, 100) : jobs.slice(100),
      })),
    ).resolves.toEqual({ total_count: 150, jobs });

    await expect(
      fetchGatewayNodeCompatProducerJobs(async (page) => ({
        total_count: 101,
        jobs: page === 1 ? jobs.slice(0, 100) : [],
      })),
    ).rejects.toThrow(/incomplete/u);

    await expect(
      fetchGatewayNodeCompatProducerJobs(async (page) => ({
        total_count: 101,
        jobs: page === 1 ? jobs.slice(0, 100) : [{ id: 100 }],
      })),
    ).rejects.toThrow(/duplicate/u);
  });

  it("accepts a positive Gateway hello outside the successful node range", () => {
    expect(
      validateGatewayNodeCompatObservation({
        outcome: "passed",
        observation: { clientMin: 3, clientMax: 4, helloProtocol: 4 },
      }),
    ).toEqual({ clientMin: 3, clientMax: 4, helloProtocol: 4 });
    expect(
      validateGatewayNodeCompatObservation({
        outcome: "passed",
        observation: { clientMin: 3, clientMax: 3, helloProtocol: 4 },
      }),
    ).toEqual({ clientMin: 3, clientMax: 3, helloProtocol: 4 });
    expect(() =>
      validateGatewayNodeCompatObservation({
        outcome: "passed",
        observation: { clientMin: 3, clientMax: 3, helloProtocol: 0 },
      }),
    ).toThrow(/positive integer/u);
    expect(() =>
      validateGatewayNodeCompatObservation({
        outcome: "protocol-mismatch",
        observation: protocolObservation({
          clientMin: 1,
          clientMax: 2,
          helloProtocol: null,
          protocolError: {
            details: {
              code: "PROTOCOL_MISMATCH",
              clientMinProtocol: 1,
              clientMaxProtocol: 2,
              expectedProtocol: 4,
            },
          },
        }),
        mismatch: {
          code: "PROTOCOL_MISMATCH",
          clientMinProtocol: 2,
          clientMaxProtocol: 2,
          expectedProtocol: 4,
        },
      }),
    ).toThrow(/does not match/u);
  });

  it("proves accepted-min 3 only from a real min-3 success and max-2 mismatch", () => {
    const drafts = buildGatewayNodeCompatCases().map((compatCase) => ({
      compatCase,
      gateway: runtimeBinding(compatCase.gateway === "candidate" ? "2026.8.6" : "2026.5.7"),
      node: runtimeBinding(compatCase.node === "candidate" ? "2026.8.6" : "2026.5.7"),
      observation:
        compatCase.outcome === "passed"
          ? protocolObservation({
              clientMax: compatCase.node === "baseline" ? 3 : 4,
            })
          : protocolObservation({
              clientMin: 1,
              clientMax: 2,
              helloProtocol: null,
              protocolError: {
                details: {
                  code: "PROTOCOL_MISMATCH",
                  clientMinProtocol: 1,
                  clientMaxProtocol: 2,
                  expectedProtocol: 4,
                },
              },
            }),
      operation:
        compatCase.outcome === "passed"
          ? ({
              method: "node.invoke",
              command: "system.which",
              params: { bins: ["node"] },
              ok: true,
              result: { bins: { node: "/usr/bin/node" } },
            } satisfies GatewayNodeCompatOperation)
          : undefined,
      mismatch:
        compatCase.outcome === "protocol-mismatch"
          ? {
              code: "PROTOCOL_MISMATCH" as const,
              clientMinProtocol: 1,
              clientMaxProtocol: 2,
              expectedProtocol: 4,
            }
          : undefined,
      startedAt: STARTED_AT,
      completedAt: COMPLETED_AT,
    }));
    expect(
      drafts.find((draft) => draft.compatCase.direction === "candidate-gateway-baseline-node")
        ?.observation,
    ).toEqual(protocolObservation({ clientMax: 3 }));
    expect(resolveGatewayNodeCompatAcceptedMin(drafts, "candidate")).toBe(3);
    expect(resolveGatewayNodeCompatAcceptedMin(drafts, "baseline")).toBe(3);
    expect(() =>
      resolveGatewayNodeCompatAcceptedMin(
        drafts.filter((draft) => draft.compatCase.direction !== "baseline-gateway-disjoint-node"),
        "baseline",
      ),
    ).toThrow(/max-2 structured mismatch/u);
    const candidateWithoutExactFloor = structuredClone(drafts);
    for (const draft of candidateWithoutExactFloor) {
      if (draft.compatCase.gateway === "candidate" && draft.compatCase.outcome === "passed") {
        draft.observation = { clientMin: 3, clientMax: 4, helloProtocol: 4 };
      }
    }
    expect(() =>
      resolveGatewayNodeCompatAcceptedMin(candidateWithoutExactFloor, "candidate"),
    ).toThrow(/\[3,3\] success/u);
  });

  it("emits canonical observed success and structured mismatch evidence", () => {
    const compatCases = buildGatewayNodeCompatCases();
    const passedCase = compatCases[0];
    const mismatchCase = compatCases[4];
    if (!passedCase || !mismatchCase) {
      throw new Error("Expected compatibility cases.");
    }
    const common = {
      gateway: runtimeBinding("2026.8.6"),
      node: runtimeBinding("2026.8.6"),
      gatewayAcceptedNodeMin: 3,
      producer,
      startedAt: STARTED_AT,
      completedAt: COMPLETED_AT,
    };
    const passed = buildGatewayNodeCompatEvidence({
      ...common,
      compatCase: passedCase,
      observation: protocolObservation(),
      operation: {
        method: "node.invoke",
        command: "system.which",
        params: { bins: ["node"] },
        ok: true,
        result: { bins: { node: "/usr/bin/node" } },
      },
    });
    const mismatch = buildGatewayNodeCompatEvidence({
      ...common,
      compatCase: mismatchCase,
      observation: protocolObservation({
        clientMin: 1,
        clientMax: 2,
        helloProtocol: null,
        protocolError: {
          details: {
            code: "PROTOCOL_MISMATCH",
            clientMinProtocol: 1,
            clientMaxProtocol: 2,
            expectedProtocol: 4,
          },
        },
      }),
      mismatch: {
        code: "PROTOCOL_MISMATCH",
        clientMinProtocol: 1,
        clientMaxProtocol: 2,
        expectedProtocol: 4,
      },
    });
    expect(validateGatewayNodeCompatEvidence(passed)).toEqual(passed);
    expect(validateGatewayNodeCompatEvidence(mismatch)).toEqual(mismatch);
    expect(canonicalizeGatewayNodeCompatEvidence(passed)).toContain(
      '"schema": "openclaw.gateway-node-compat/v1"',
    );
    expect(passed).toMatchObject({
      connection: { role: "node", mode: "node" },
      node: {
        kind: "linux",
        architecture: resolveGatewayNodeCompatArchitecture(process.arch),
        protocolClientId: "node-host",
      },
    });
    expect(() =>
      buildGatewayNodeCompatEvidence({
        ...common,
        compatCase: passedCase,
        observation: protocolObservation({ role: "operator" }),
        operation: passed.operation!,
      }),
    ).toThrow(/Observed Gateway connect identity/u);
  });

  it("approves only the exact pending node id and display identity", () => {
    const pending = [
      { requestId: "wrong-id", nodeId: "other", displayName: "linux-case" },
      { requestId: "wrong-name", nodeId: "linux-case", displayName: "other" },
      { requestId: "match", nodeId: "linux-case", displayName: "linux-case" },
    ];
    expect(selectExpectedPendingNodeRequest(pending, "linux-case", "linux-case")).toBe("match");
    expect(selectExpectedPendingNodeRequest(pending, "missing", "missing")).toBeNull();
    expect(() =>
      selectExpectedPendingNodeRequest(
        [...pending, { requestId: "duplicate", nodeId: "linux-case", displayName: "linux-case" }],
        "linux-case",
        "linux-case",
      ),
    ).toThrow(/Multiple pending requests/u);
  });

  it("recursively rejects token leaks and unexpected uploaded files", () => {
    const root = mkdtempSync(join(tmpdir(), "gateway-node-artifact-"));
    try {
      for (const compatCase of buildGatewayNodeCompatCases()) {
        writeFileSync(join(root, `${compatCase.caseId}.json`), "{}\n");
      }
      expect(() => assertGatewayNodeCompatArtifactSafe(root, ["secret-token"])).not.toThrow();
      writeFileSync(join(root, `${buildGatewayNodeCompatCases()[0]!.caseId}.json`), "secret-token");
      expect(() => assertGatewayNodeCompatArtifactSafe(root, ["secret-token"])).toThrow(
        /Gateway token leaked/u,
      );
      mkdirSync(join(root, "logs"));
      writeFileSync(join(root, "logs", "gateway.log"), "not uploadable");
      expect(() => assertGatewayNodeCompatArtifactSafe(root, [])).toThrow(/unexpected files/u);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("forwards frames through the observer, records protocol, and releases its port", async () => {
    const upstreamPort = await reservePort();
    const observerPort = await reservePort();
    await withGatewayNodeCompatCleanup(async (own) => {
      const upstream = new WebSocketServer({
        host: "127.0.0.1",
        port: upstreamPort,
        verifyClient: (_info, done) => setTimeout(() => done(true), 25),
      });
      own(() => closeWebSocketServer(upstream));
      await once(upstream, "listening");
      const upstreamFrame = new Promise<Record<string, unknown>>((resolvePromise) => {
        upstream.once("connection", (socket) => {
          socket.once("message", (data) => {
            const text = Array.isArray(data)
              ? Buffer.concat(data.map((part) => Buffer.from(part))).toString("utf8")
              : Buffer.isBuffer(data)
                ? data.toString("utf8")
                : Buffer.from(new Uint8Array(data)).toString("utf8");
            const frame = JSON.parse(text) as Record<string, unknown>;
            resolvePromise(frame);
            socket.send(
              JSON.stringify({
                id: frame.id,
                payload: { type: "hello-ok", protocol: 4 },
              }),
            );
          });
        });
      });
      const observer = await startProtocolObserver({
        port: observerPort,
        upstreamUrl: `ws://127.0.0.1:${upstreamPort}`,
        own,
      });
      const client = new WebSocket(`ws://127.0.0.1:${observerPort}`);
      client.on("error", () => {});
      await once(client, "open");
      client.send(
        JSON.stringify({
          id: "connect-1",
          method: "connect",
          params: {
            minProtocol: 3,
            maxProtocol: 4,
            role: "node",
            client: { id: "node-host", mode: "node", platform: "linux" },
          },
        }),
      );
      const [response] = await once(client, "message");
      expect(JSON.parse(response.toString())).toMatchObject({
        id: "connect-1",
        payload: { type: "hello-ok", protocol: 4 },
      });
      await expect(upstreamFrame).resolves.toMatchObject({
        id: "connect-1",
        method: "connect",
      });
      expect(observer.read()).toEqual(
        protocolObservation({ architecture: resolveGatewayNodeCompatArchitecture(process.arch) }),
      );
      client.close();
      await once(client, "close");
    });
    expect(await canConnectToLoopbackPort(observerPort)).toBe(false);
  }, 10_000);

  it("uses the observed Gateway error frame instead of spoofed client mismatch details", async () => {
    const upstreamPort = await reservePort();
    const observerPort = await reservePort();
    await withGatewayNodeCompatCleanup(async (own) => {
      const upstream = new WebSocketServer({ host: "127.0.0.1", port: upstreamPort });
      own(() => closeWebSocketServer(upstream));
      await once(upstream, "listening");
      upstream.once("connection", (socket) => {
        socket.once("message", (data) => {
          const frame = JSON.parse(data.toString()) as { id: string };
          socket.send(
            JSON.stringify({
              id: frame.id,
              ok: false,
              error: {
                code: "INVALID_REQUEST",
                message: "protocol mismatch",
                details: {
                  code: "PROTOCOL_MISMATCH",
                  clientMinProtocol: 1,
                  clientMaxProtocol: 2,
                  expectedProtocol: 4,
                },
              },
            }),
          );
        });
      });
      const observer = await startProtocolObserver({
        port: observerPort,
        upstreamUrl: `ws://127.0.0.1:${upstreamPort}`,
        own,
      });
      const client = new WebSocket(`ws://127.0.0.1:${observerPort}`);
      client.on("error", () => {});
      await once(client, "open");
      client.send(
        JSON.stringify({
          id: "connect-spoof",
          method: "connect",
          params: {
            minProtocol: 1,
            maxProtocol: 2,
            role: "node",
            client: { id: "node-host", mode: "node", platform: "linux" },
          },
        }),
      );
      await once(client, "message");
      const spoofedClientDetails = {
        code: "PROTOCOL_MISMATCH",
        clientMinProtocol: 99,
        clientMaxProtocol: 99,
        expectedProtocol: 99,
      };
      expect(spoofedClientDetails.expectedProtocol).toBe(99);
      expect(normalizeProtocolMismatch({ observation: observer.read() })).toEqual({
        code: "PROTOCOL_MISMATCH",
        clientMinProtocol: 1,
        clientMaxProtocol: 2,
        expectedProtocol: 4,
      });
      client.close();
      await once(client, "close");
    });
  }, 10_000);

  it("reports the latest successful connect attempt after a transient Gateway error", async () => {
    const upstreamPort = await reservePort();
    const observerPort = await reservePort();
    await withGatewayNodeCompatCleanup(async (own) => {
      const upstream = new WebSocketServer({ host: "127.0.0.1", port: upstreamPort });
      own(() => closeWebSocketServer(upstream));
      await once(upstream, "listening");
      upstream.once("connection", (socket) => {
        let attempt = 0;
        socket.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as { id: string };
          attempt += 1;
          socket.send(
            JSON.stringify(
              attempt === 1
                ? {
                    id: frame.id,
                    ok: false,
                    error: {
                      code: "INVALID_REQUEST",
                      message: "pairing required",
                      details: { code: "PAIRING_REQUIRED" },
                    },
                  }
                : {
                    id: frame.id,
                    ok: true,
                    payload: { type: "hello-ok", protocol: 4 },
                  },
            ),
          );
        });
      });
      const observer = await startProtocolObserver({
        port: observerPort,
        upstreamUrl: `ws://127.0.0.1:${upstreamPort}`,
        own,
      });
      const client = new WebSocket(`ws://127.0.0.1:${observerPort}`);
      client.on("error", () => {});
      await once(client, "open");
      const connectParams = {
        minProtocol: 3,
        maxProtocol: 4,
        role: "node",
        client: { id: "node-host", mode: "node", platform: "linux" },
      };
      client.send(
        JSON.stringify({ id: "connect-first", method: "connect", params: connectParams }),
      );
      await once(client, "message");
      client.send(
        JSON.stringify({ id: "connect-second", method: "connect", params: connectParams }),
      );
      await once(client, "message");
      expect(observer.read()).toEqual(
        protocolObservation({ architecture: resolveGatewayNodeCompatArchitecture(process.arch) }),
      );
      client.close();
      await once(client, "close");
    });
  }, 10_000);

  it("closes the downstream session when the observer cannot reach its upstream", async () => {
    const unavailablePort = await reservePort();
    const observerPort = await reservePort();
    await withGatewayNodeCompatCleanup(async (own) => {
      await startProtocolObserver({
        port: observerPort,
        upstreamUrl: `ws://127.0.0.1:${unavailablePort}`,
        own,
      });
      const client = new WebSocket(`ws://127.0.0.1:${observerPort}`);
      client.on("error", () => {});
      await once(client, "open");
      await once(client, "close");
    });
    expect(await canConnectToLoopbackPort(observerPort)).toBe(false);
  }, 10_000);

  it.each(["install", "startup"])("cleans owned state after %s failure", async (phase) => {
    const root = mkdtempSync(join(tmpdir(), `gateway-node-${phase}-`));
    await expect(
      withGatewayNodeCompatCleanup(async (own) => {
        own(() => rmSync(root, { recursive: true, force: true }));
        throw new Error(`${phase} failed`);
      }),
    ).rejects.toThrow(`${phase} failed`);
    expect(existsSync(root)).toBe(false);
  });

  it("surfaces cleanup failure after a successful body", async () => {
    await expect(
      withGatewayNodeCompatCleanup(async (own) => {
        own(() => {
          throw new Error("cleanup failed");
        });
        return "ok";
      }),
    ).rejects.toThrow(/cleanup failed/u);
  });

  it("preserves the body failure when cleanup also fails", async () => {
    await expect(
      withGatewayNodeCompatCleanup(async (own) => {
        own(() => {
          throw new Error("cleanup failed");
        });
        throw new Error("body failed");
      }),
    ).rejects.toThrow("body failed");
  });

  it.skipIf(process.env.OPENCLAW_GATEWAY_NODE_PUBLISHED_SMOKE !== "1")(
    "starts the real published v2026.5.7 Gateway with packaged module paths",
    async () => {
      await withGatewayNodeCompatCleanup(async (own) => {
        const root = mkdtempSync(join(tmpdir(), "gateway-node-published-"));
        own(() => rmSync(root, { recursive: true, force: true }));
        const prefixDir = join(root, "prefix");
        const homeDir = join(root, "home");
        const stateDir = join(homeDir, ".openclaw");
        mkdirSync(prefixDir, { recursive: true });
        mkdirSync(stateDir, { recursive: true });
        const lane = {
          name: "published-v3",
          rootDir: root,
          prefixDir,
          homeDir,
          stateDir,
          appDataDir: stateDir,
          gatewayPort: 0,
          phaseTimings: [],
        };
        const env = {
          ...process.env,
          HOME: homeDir,
          OPENCLAW_HOME: homeDir,
          OPENCLAW_STATE_DIR: stateDir,
          OPENCLAW_CONFIG_PATH: join(stateDir, "openclaw.json"),
          OPENCLAW_GATEWAY_TOKEN: "published-smoke-token",
          OPENCLAW_DISABLE_BONJOUR: "1",
          NPM_CONFIG_PREFIX: prefixDir,
          PATH: `${binDirForPrefix(prefixDir)}:${process.env.PATH ?? ""}`,
        };
        const pack = await runCommand(
          npmCommand(),
          [
            "pack",
            "--ignore-scripts",
            "--json",
            GATEWAY_NODE_COMPAT_BASELINE_SPEC,
            "--pack-destination",
            root,
          ],
          { logPath: join(root, "pack.log"), timeoutMs: 5 * 60_000 },
        );
        const packed = JSON.parse(pack.stdout) as Array<{ filename?: unknown }>;
        const filename = packed[0]?.filename;
        expect(packed).toHaveLength(1);
        expect(typeof filename === "string" && basename(filename) === filename).toBe(true);
        await installTarballPackage({
          lane,
          env,
          tgzPath: join(root, filename as string),
          logPath: join(root, "install.log"),
        });
        expect(readInstalledMetadata(prefixDir).version).toBe("2026.5.7");
        const packageRoot = dirname(installedEntryPath(prefixDir));
        expect(existsSync(join(packageRoot, "dist", "plugin-sdk", "gateway-runtime.js"))).toBe(
          true,
        );
        expect(createRequire(join(packageRoot, "package.json")).resolve("ws")).toContain(
          "node_modules",
        );

        const port = await reservePort();
        const args = buildGatewayNodeCompatGatewayArgs(port);
        expect(args).toContain("--allow-unconfigured");
        const invocation = resolveInstalledCliInvocation(
          join(binDirForPrefix(prefixDir), "openclaw"),
          args,
          { env },
        );
        const logPath = join(root, "gateway.log");
        const logFd = openSync(logPath, "a");
        const child = spawn(invocation.command, invocation.args, {
          cwd: homeDir,
          env,
          detached: true,
          shell: invocation.shell,
          stdio: ["ignore", logFd, logFd],
        });
        const activeTree = registerActiveChildProcessTree(child);
        own(async () => {
          await stopGateway({
            child,
            logPath,
            closeLog: async () => {
              activeTree.unregister();
              closeSync(logFd);
            },
          });
        });
        const deadline = Date.now() + 30_000;
        while (!(await canConnectToLoopbackPort(port)) && Date.now() < deadline) {
          if (child.exitCode !== null) {
            throw new Error(readFileSync(logPath, "utf8"));
          }
          await new Promise((resolvePromise) => {
            setTimeout(resolvePromise, 250);
          });
        }
        expect(await canConnectToLoopbackPort(port)).toBe(true);
      });
    },
    6 * 60_000,
  );
});

async function reservePort() {
  return new Promise<number>((resolvePromise, rejectPromise) => {
    const server = createServer();
    server.once("error", rejectPromise);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        rejectPromise(new Error("Failed to reserve test port."));
        return;
      }
      server.close((error) => (error ? rejectPromise(error) : resolvePromise(address.port)));
    });
  });
}

async function closeWebSocketServer(server: WebSocketServer) {
  for (const client of server.clients) {
    client.terminate();
  }
  await new Promise<void>((resolvePromise, rejectPromise) => {
    server.close((error) => (error ? rejectPromise(error) : resolvePromise()));
  });
}
