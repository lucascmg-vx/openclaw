import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import {
  chmodSync,
  chownSync,
  closeSync,
  cpSync,
  createWriteStream,
  fstatSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  readlinkSync,
  readSync,
  readdirSync,
  rmSync,
  writeFileSync,
  writeSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import type { RawData } from "ws";
import type {
  GatewayNodeCompatActionsArtifact,
  GatewayNodeCompatDirection,
  GatewayNodeCompatEvidence,
  GatewayNodeCompatOperation,
  GatewayNodeCompatRuntimeBinding,
} from "../../gateway-node-compat-evidence.mjs";
import {
  canonicalizeGatewayNodeCompatEvidence,
  validateGatewayNodeCompatEvidence,
} from "../../gateway-node-compat-evidence.mjs";
import {
  validateActionsArtifactBinding,
  validateActionsArtifactProducerJob,
  type ArtifactBinding,
} from "../actions-artifact-archive.mjs";
import type {
  Cleanup,
  LaneState,
  ParsedArgs,
  ProcessIdentity,
  StoppableProcessHandle,
} from "./config.ts";
import { GATEWAY_NODE_COMPAT_BASELINE_VERSION } from "./config.ts";
import {
  binDirForPrefix,
  installTarballPackage,
  installedEntryPath,
  npmCommand,
  readInstalledMetadata,
} from "./install.ts";
import { resolveInstalledCliInvocation } from "./installed.ts";
import { readBoundedCrossOsResponseText } from "./network-smokes.ts";
import {
  canConnectToLoopbackPort,
  registerActiveChildProcessTree,
  runCleanup,
  runCommand,
  runCommandInvocation,
  stopGateway,
  withAllocatedGatewayPort,
} from "./process.ts";
import { formatError, sleep } from "./shared.ts";

const SCHEMA = "openclaw.gateway-node-compat/v1";
const REUSABLE_WORKFLOW_PATH = ".github/workflows/openclaw-cross-os-release-checks-reusable.yml";
const TIMEOUT_MS = 2 * 60_000;
const API_JSON_LIMIT = 2 * 1024 * 1024;
const JOBS_PAGE_SIZE = 100;
const MAX_JOB_PAGES = 10;
const DISJOINT_MIN_PROTOCOL = 1;
const DISJOINT_MAX_PROTOCOL = 2;
const PROVEN_GATEWAY_ACCEPTED_NODE_MIN = 3;
const BIN = "node";
const GATEWAY_NODE_COMPAT_EXECUTION_PLAN_SCHEMA = "openclaw.gateway-node-compat-plan/v1";
export const GATEWAY_NODE_COMPAT_CONTAINER_IMAGE =
  "node:24-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d";
const GATEWAY_NODE_COMPAT_CONTAINER_PLAN_PATH = "/openclaw-plan.json";
const GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_PATH = "/openclaw-candidate.tgz";
const GATEWAY_NODE_COMPAT_CONTAINER_BASELINE_PATH = "/openclaw-baseline.tgz";
const GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH = "/openclaw-prepared";
const GATEWAY_NODE_COMPAT_CONTAINER_OUTPUT_PATH = "/openclaw-output";
const GATEWAY_NODE_COMPAT_CONTAINER_DIAGNOSTICS_PATH = "/openclaw-diagnostics";
const GATEWAY_NODE_COMPAT_CONTAINER_HOST_UID_ENV = "OPENCLAW_GATEWAY_NODE_COMPAT_HOST_UID";
const GATEWAY_NODE_COMPAT_CONTAINER_HOST_GID_ENV = "OPENCLAW_GATEWAY_NODE_COMPAT_HOST_GID";
const GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_UID_ENV =
  "OPENCLAW_GATEWAY_NODE_COMPAT_CANDIDATE_UID";
const GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_GID_ENV =
  "OPENCLAW_GATEWAY_NODE_COMPAT_CANDIDATE_GID";
const GATEWAY_NODE_COMPAT_DEFAULT_CANDIDATE_UID = 65_532;
const GATEWAY_NODE_COMPAT_PREPARED_SCHEMA = "openclaw.gateway-node-compat-prepared/v1";
const GATEWAY_NODE_COMPAT_MAX_EVIDENCE_BYTES = 64 * 1024;
const GATEWAY_NODE_COMPAT_MAX_PLAN_BYTES = 64 * 1024;
const GATEWAY_NODE_COMPAT_MAX_DIAGNOSTIC_BYTES = 64 * 1024;
const GATEWAY_NODE_COMPAT_PREPARED_MANIFEST = "manifest.json";
const GATEWAY_NODE_COMPAT_INSTALL_NODE_OPTIONS = "--max-old-space-size=5120";
const GATEWAY_NODE_COMPAT_DOCKER_PHASE_TIMEOUT_MS = 30 * 60_000;
const GATEWAY_NODE_COMPAT_CONTAINER_CLEANUP_TIMEOUT_MS = 30_000;
const GATEWAY_NODE_COMPAT_CONTAINER_NAME_RE =
  /^openclaw-gateway-node-compat-(prepare|runtime)-[1-9][0-9]*-[a-f0-9]{16}$/u;
const GATEWAY_NODE_COMPAT_PREPARE_ENTRYPOINT = `
const compat = await import("file:///workflow/scripts/lib/cross-os-release-checks/gateway-node-compat.ts");
await compat.prepareGatewayNodeLinuxCompatContainer(
  compat.readGatewayNodeCompatExecutionPlan("/openclaw-plan.json"),
);
`.trim();
const GATEWAY_NODE_COMPAT_CONTAINER_ENTRYPOINT = `
const compat = await import("file:///workflow/scripts/lib/cross-os-release-checks/gateway-node-compat.ts");
await compat.runGatewayNodeLinuxCompatContainer(
  compat.readGatewayNodeCompatExecutionPlan("/openclaw-plan.json"),
);
`.trim();
const WORKFLOW_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

type RuntimeId = "baseline" | "candidate";
type Outcome = "passed" | "protocol-mismatch";
type CleanupOwner = (cleanup: Cleanup) => void;

type ArtifactSelection = Omit<GatewayNodeCompatActionsArtifact, "sizeBytes">;
export type GatewayNodeCompatPackageExecutionInput = GatewayNodeCompatPackageInput & {
  artifactFileName: string;
};

export type GatewayNodeCompatExecutionPlan = {
  schema: typeof GATEWAY_NODE_COMPAT_EXECUTION_PLAN_SCHEMA;
  candidate: GatewayNodeCompatPackageExecutionInput;
  baseline: GatewayNodeCompatPackageExecutionInput;
  producer: GatewayNodeCompatRunParams["producer"];
  outputDir: typeof GATEWAY_NODE_COMPAT_CONTAINER_OUTPUT_PATH;
};

type GatewayNodeCompatPreparedManifest = {
  schema: typeof GATEWAY_NODE_COMPAT_PREPARED_SCHEMA;
  candidate: { treeSha256: string };
  baseline: { treeSha256: string };
};

export type GatewayNodeCompatCase = {
  caseId: string;
  direction: GatewayNodeCompatDirection;
  gateway: RuntimeId;
  node: RuntimeId;
  outcome: Outcome;
};

export type GatewayNodeCompatPackageSelection = ReturnType<typeof parsePackageSelection>;

type GatewayNodeCompatPackageInput = Omit<GatewayNodeCompatPackageSelection, "actionsArtifact"> & {
  actionsArtifact: GatewayNodeCompatActionsArtifact;
  tarballSizeBytes: number;
};
type GatewayNodeCompatArtifactBoundPackageInput = Omit<
  GatewayNodeCompatPackageInput,
  "tarballSizeBytes"
>;

export type GatewayNodeCompatRunParams = ReturnType<typeof parseGatewayNodeCompatRunParams>;
type ActionsContext = GatewayNodeCompatRunParams["actions"];

type InstalledCompatRuntime = {
  lane: LaneState;
  prefixDir: string;
  packageRoot: string;
  cliPath: string;
  processIdentity?: ProcessIdentity;
  binding: GatewayNodeCompatRuntimeBinding;
};

type GatewayNodeCompatArchitecture = "arm64" | "x64";

type ObservedConnectIdentity = {
  architecture: GatewayNodeCompatArchitecture;
  clientId: string;
  mode: string;
  platform: string;
  role: string;
};

type ProtocolObservation = {
  clientMin: number;
  clientMax: number;
  helloProtocol: number | null;
  identity?: ObservedConnectIdentity;
  protocolError?: unknown | null;
};

type ProtocolMismatch = {
  code: "PROTOCOL_MISMATCH";
  clientMinProtocol: number;
  clientMaxProtocol: number;
  expectedProtocol: number;
};

type CaseDraft = {
  compatCase: GatewayNodeCompatCase;
  gateway: GatewayNodeCompatRuntimeBinding;
  node: GatewayNodeCompatRuntimeBinding;
  observation: ProtocolObservation;
  operation?: GatewayNodeCompatOperation;
  mismatch?: ProtocolMismatch;
  startedAt: string;
  completedAt: string;
};
type CaseRunParams = {
  compatCase: GatewayNodeCompatCase;
  gateway: InstalledCompatRuntime;
  node: InstalledCompatRuntime;
  logsDir: string;
  own: CleanupOwner;
  token: string;
};

type PendingNodeRequest = { requestId?: unknown; nodeId?: unknown; displayName?: unknown };

export function buildGatewayNodeCompatCases(
  architecture: GatewayNodeCompatArchitecture = resolveGatewayNodeCompatArchitecture(process.arch),
): GatewayNodeCompatCase[] {
  const cases = [
    ["candidate-gateway-candidate-node", "candidate", "candidate", "passed"],
    ["candidate-gateway-baseline-node", "candidate", "baseline", "passed"],
    ["baseline-gateway-candidate-node", "baseline", "candidate", "passed"],
    ["baseline-gateway-baseline-node", "baseline", "baseline", "passed"],
    ["candidate-gateway-disjoint-node", "candidate", "candidate", "protocol-mismatch"],
    ["baseline-gateway-disjoint-node", "baseline", "candidate", "protocol-mismatch"],
  ] as const;
  return cases.map(([direction, gateway, node, outcome]) => ({
    caseId: `linux-${architecture}-${direction}`,
    direction,
    gateway,
    node,
    outcome,
  })) as GatewayNodeCompatCase[];
}

export function buildGatewayNodeCompatGatewayArgs(port: number) {
  return ["gateway", "run", "--bind", "loopback", "--port", String(port)].concat(
    "--force",
    "--allow-unconfigured",
  );
}

export function buildGatewayNodeCompatNodeArgs(port: number, caseId: string) {
  return ["node", "run", "--host", "127.0.0.1", "--port", String(port)].concat(
    "--node-id",
    caseId,
    "--display-name",
    caseId,
  );
}

export function buildGatewayNodeCompatInvokeArgs(params: { gatewayUrl: string; nodeId: string }) {
  return ["nodes", "invoke", "--node", params.nodeId, "--command", "system.which"].concat(
    "--params",
    JSON.stringify({ bins: [BIN] }),
    "--json",
    "--url",
    params.gatewayUrl,
  );
}

export function parseGatewayNodeCompatRunParams(args: ParsedArgs, env = process.env) {
  const repository = requireValue(env.GITHUB_REPOSITORY, "GITHUB_REPOSITORY");
  const runId = requirePattern(env.GITHUB_RUN_ID, "GITHUB_RUN_ID", /^[1-9][0-9]*$/u);
  const runAttempt = requirePositiveInteger(env.GITHUB_RUN_ATTEMPT, "GITHUB_RUN_ATTEMPT");
  const workflowPath = parseGatewayNodeCompatWorkflowPath(
    requireValue(env.GITHUB_WORKFLOW_REF, "GITHUB_WORKFLOW_REF"),
    repository,
  );
  return {
    outputDir: resolveRequiredPath(args, "output-dir"),
    candidate: parsePackageSelection(args, "candidate", runId, runAttempt, true),
    baseline: parsePackageSelection(args, "compat-baseline", runId, runAttempt, false),
    producer: {
      repository,
      workflowSha: requirePattern(
        env.GATEWAY_NODE_COMPAT_WORKFLOW_SHA,
        "GATEWAY_NODE_COMPAT_WORKFLOW_SHA",
        /^[a-f0-9]{40}$/u,
      ),
      runId,
      runAttempt,
      job: requireValue(env.GITHUB_JOB, "GITHUB_JOB"),
    },
    actions: {
      apiUrl: requireValue(env.GITHUB_API_URL ?? "https://api.github.com", "GITHUB_API_URL"),
      token: requireValue(env.GITHUB_TOKEN, "GITHUB_TOKEN"),
      repository,
      headSha: requirePattern(env.GITHUB_SHA, "GITHUB_SHA", /^[a-f0-9]{40}$/u),
      headBranch: requireValue(env.GITHUB_HEAD_REF || env.GITHUB_REF_NAME, "GitHub head branch"),
      event: requireValue(env.GITHUB_EVENT_NAME, "GITHUB_EVENT_NAME"),
      consumerRunAttempt: runAttempt,
      consumerRunId: runId,
      workflowPath,
    },
  };
}

function parseGatewayNodeCompatWorkflowPath(workflowRef: string, repository: string) {
  const prefix = `${repository}/`;
  const separator = workflowRef.lastIndexOf("@");
  if (
    !workflowRef.startsWith(prefix) ||
    separator <= prefix.length ||
    separator === workflowRef.length - 1
  ) {
    throw new Error("GITHUB_WORKFLOW_REF must identify the caller workflow and ref.");
  }
  const workflowPath = workflowRef.slice(prefix.length, separator);
  if (!/^\.github\/workflows\/[A-Za-z0-9][A-Za-z0-9_.-]*\.ya?ml$/u.test(workflowPath)) {
    throw new Error("GITHUB_WORKFLOW_REF contains an invalid caller workflow path.");
  }
  return workflowPath;
}

function parsePackageSelection(
  args: ParsedArgs,
  prefix: "candidate" | "compat-baseline",
  currentRunId: string,
  consumerRunAttempt: number,
  allowPriorRun: boolean,
) {
  const runId = requirePattern(
    args[`${prefix}-artifact-run-id`],
    `${prefix}-artifact-run-id`,
    /^[1-9][0-9]*$/u,
  );
  const runAttempt = requirePositiveInteger(
    args[`${prefix}-artifact-run-attempt`],
    `${prefix}-artifact-run-attempt`,
  );
  if (!allowPriorRun && runId !== currentRunId) {
    throw new Error(`${prefix} artifact must come from the current workflow run.`);
  }
  if (runId === currentRunId && runAttempt > consumerRunAttempt) {
    throw new Error(`${prefix} artifact attempt must not be newer than the consumer attempt.`);
  }
  const digest = requirePattern(
    args[`${prefix}-artifact-digest`],
    `${prefix}-artifact-digest`,
    /^(?:sha256:)?[a-f0-9]{64}$/u,
  );
  return {
    tgzPath: resolveRequiredPath(args, `${prefix}-tgz`),
    version: requireValue(args[`${prefix}-version`], `${prefix}-version`),
    sourceSha: requirePattern(
      args[`${prefix}-source-sha`],
      `${prefix}-source-sha`,
      /^[a-f0-9]{40}$/u,
    ),
    sha256: requirePattern(args[`${prefix}-sha256`], `${prefix}-sha256`, /^[a-f0-9]{64}$/u),
    actionsArtifact: {
      id: requirePositiveInteger(args[`${prefix}-artifact-id`], `${prefix}-artifact-id`),
      name:
        prefix === "candidate"
          ? requireValue(args[`${prefix}-artifact-name`], `${prefix}-artifact-name`)
          : `openclaw-gateway-node-compat-baseline-${runId}-${runAttempt}`,
      digest: digest.startsWith("sha256:") ? (digest as `sha256:${string}`) : `sha256:${digest}`,
      runId,
      runAttempt,
    } satisfies ArtifactSelection,
  };
}

export function validateGatewayNodeCompatArtifactBinding(params: {
  selection: GatewayNodeCompatPackageSelection;
  actions: ActionsContext;
  artifactMetadata: unknown;
  workflowRun: unknown;
  workflowJobs: unknown;
}): GatewayNodeCompatArtifactBoundPackageInput {
  const sizeBytes = asRecord(params.artifactMetadata).size_in_bytes;
  if (!Number.isSafeInteger(sizeBytes) || (sizeBytes as number) < 1) {
    throw new Error("Actions artifact size_in_bytes must be a positive integer.");
  }
  const artifact = params.selection.actionsArtifact;
  const producerJobName = resolveGatewayNodeCompatProducerJobName({
    artifactName: artifact.name,
    workflowPath: params.actions.workflowPath,
    workflowJobs: params.workflowJobs,
  });
  const expected: ArtifactBinding = {
    artifactDigest: artifact.digest,
    artifactId: artifact.id,
    artifactName: artifact.name,
    artifactSizeBytes: sizeBytes as number,
    repository: params.actions.repository,
    runStatePolicy: "same-run-producer-success",
    runAttempt: artifact.runAttempt,
    runId: Number(artifact.runId),
    workflowEvent: params.actions.event,
    workflowHeadBranch: params.actions.headBranch,
    workflowPath: params.actions.workflowPath,
    workflowSha: params.actions.headSha,
    consumerRunAttempt: params.actions.consumerRunAttempt,
    consumerRunId: Number(params.actions.consumerRunId),
    producerJobName,
  };
  validateActionsArtifactBinding({
    artifactMetadata: params.artifactMetadata,
    expected,
    workflowRun: params.workflowRun,
  });
  validateActionsArtifactProducerJob({ expected, workflowJobs: params.workflowJobs });
  return {
    ...params.selection,
    actionsArtifact: { ...artifact, sizeBytes: sizeBytes as number },
  };
}

function resolveGatewayNodeCompatProducerJobName(params: {
  artifactName: string;
  workflowPath: string;
  workflowJobs: unknown;
}) {
  const jobs = asRecord(params.workflowJobs).jobs;
  if (!Array.isArray(jobs)) {
    throw new Error("Actions workflow jobs inventory is incomplete.");
  }
  if (params.artifactName.startsWith("release-package-under-test-")) {
    const matches = jobs
      .map((job) => asRecord(job).name)
      .filter(
        (name): name is string =>
          typeof name === "string" && /(?:^| \/ )Prepare release package artifact$/u.test(name),
      );
    if (matches.length !== 1) {
      throw new Error("Release package artifact producer job must be unique.");
    }
    return matches[0];
  }
  if (params.workflowPath === REUSABLE_WORKFLOW_PATH) {
    return "prepare";
  }
  const matches = jobs
    .map((job) => asRecord(job).name)
    .filter((name): name is string => typeof name === "string" && /^.+ \/ prepare$/u.test(name));
  if (matches.length !== 1) {
    throw new Error("Called workflow prepare producer job must be unique.");
  }
  return matches[0];
}

export async function runGatewayNodeLinuxCompat(params: GatewayNodeCompatRunParams) {
  assertGatewayNodeCompatPlatform();
  assertGatewayNodeCompatBaseline(params.baseline.version);

  await withGatewayNodeCompatCleanup(async (own) => {
    const workDir = createOwnedDirectory("gateway-node-compat-host", own);
    const planPath = join(workDir, "plan.json");
    const preparedDir = join(workDir, "prepared");
    const containerOutputDir = join(workDir, "container-output");
    const prepareLogPath = join(workDir, "prepare-container.log");
    const runtimeLogPath = join(workDir, "runtime-container.log");
    const diagnosticsDir = gatewayNodeCompatDiagnosticsDir(params.outputDir);
    mkdirSync(preparedDir, { recursive: true, mode: 0o700 });
    mkdirSync(containerOutputDir, { recursive: true, mode: 0o700 });
    rmSync(diagnosticsDir, { recursive: true, force: true });
    mkdirSync(diagnosticsDir, { recursive: true, mode: 0o700 });

    const [candidateInput, baselineInput] = await validateGatewayNodeCompatPackageInputs(params);
    assertGatewayNodeCompatPackageHash("candidate", candidateInput);
    assertGatewayNodeCompatPackageHash("baseline", baselineInput);
    const plan = buildGatewayNodeCompatExecutionPlan({
      candidate: candidateInput,
      baseline: baselineInput,
      producer: params.producer,
    });
    writeFileSync(planPath, canonicalizeGatewayNodeCompatExecutionPlan(plan), {
      encoding: "utf8",
      mode: 0o600,
    });

    const prepareContainerName = createGatewayNodeCompatContainerName("prepare");
    await runGatewayNodeCompatDockerPhase({
      phase: "prepare",
      args: buildGatewayNodeCompatPreparationDockerArgs({
        containerName: prepareContainerName,
        planPath,
        candidateTgzPath: candidateInput.tgzPath,
        baselineTgzPath: baselineInput.tgzPath,
        diagnosticsDir,
        preparedDir,
        workflowRoot: WORKFLOW_ROOT,
      }),
      containerName: prepareContainerName,
      logPath: prepareLogPath,
      outputDir: params.outputDir,
      timeoutMs: GATEWAY_NODE_COMPAT_DOCKER_PHASE_TIMEOUT_MS,
    });
    validateGatewayNodeCompatPreparedRuntime(preparedDir);

    const runtimeContainerName = createGatewayNodeCompatContainerName("runtime");
    await runGatewayNodeCompatDockerPhase({
      phase: "runtime",
      args: buildGatewayNodeCompatDockerArgs({
        containerName: runtimeContainerName,
        plan,
        planPath,
        diagnosticsDir,
        preparedDir,
        outputDir: containerOutputDir,
        workflowRoot: WORKFLOW_ROOT,
      }),
      containerName: runtimeContainerName,
      logPath: runtimeLogPath,
      outputDir: params.outputDir,
      timeoutMs: GATEWAY_NODE_COMPAT_DOCKER_PHASE_TIMEOUT_MS,
    });
    const canonicalEvidence = validateGatewayNodeCompatContainerEvidence(containerOutputDir, plan);
    rmSync(params.outputDir, { recursive: true, force: true });
    mkdirSync(params.outputDir, { recursive: true });
    for (const [fileName, contents] of canonicalEvidence) {
      writeFileSync(join(params.outputDir, fileName), contents, {
        encoding: "utf8",
        mode: 0o600,
      });
    }
  });
}

function gatewayNodeCompatDiagnosticsDir(outputDir: string) {
  return join(dirname(outputDir), "diagnostics");
}

function writeGatewayNodeCompatFailureDiagnostic(
  outputDir: string,
  phase: "prepare" | "runtime",
  logPath: string,
  exitCode: number | undefined,
  error?: unknown,
) {
  const diagnosticsDir = gatewayNodeCompatDiagnosticsDir(outputDir);
  mkdirSync(diagnosticsDir, { recursive: true, mode: 0o700 });
  const rawTail = readGatewayNodeCompatBoundedTail(
    logPath,
    GATEWAY_NODE_COMPAT_MAX_DIAGNOSTIC_BYTES,
  );
  const diagnostic = boundGatewayNodeCompatDiagnostic(
    `phase=${phase}\nexitCode=${exitCode ?? "unavailable"}\nerror=${
      error === undefined ? "none" : formatError(error)
    }\n${rawTail}\n`,
  );
  writeFileSync(join(diagnosticsDir, `${phase}-host.log`), diagnostic, {
    encoding: "utf8",
    mode: 0o600,
  });
}

type GatewayNodeCompatDockerCommand = typeof runCommand;

export async function runGatewayNodeCompatDockerPhase(
  params: {
    phase: "prepare" | "runtime";
    args: string[];
    containerName: string;
    logPath: string;
    outputDir: string;
    timeoutMs: number;
  },
  runCommandImpl: GatewayNodeCompatDockerCommand = runCommand,
) {
  assertGatewayNodeCompatContainerName(params.containerName);
  mkdirSync(dirname(params.logPath), { recursive: true });
  writeFileSync(params.logPath, "", { encoding: "utf8", flag: "a", mode: 0o600 });
  let result: Awaited<ReturnType<GatewayNodeCompatDockerCommand>> | undefined;
  let commandError: unknown;
  let cleanupError: unknown;
  const cleanupLogPath = join(dirname(params.logPath), `${params.phase}-container-cleanup.log`);
  const removeSignalCleanup = installGatewayNodeCompatSignalCleanup({
    containerName: params.containerName,
    logPath: cleanupLogPath,
    outputDir: params.outputDir,
    phase: params.phase,
  });
  try {
    try {
      result = await runCommandImpl("docker", params.args, {
        logPath: params.logPath,
        timeoutMs: params.timeoutMs,
        check: false,
      });
    } catch (error) {
      commandError = error;
    }
  } finally {
    try {
      await removeGatewayNodeCompatContainer(params.containerName, cleanupLogPath, runCommandImpl);
    } catch (error) {
      cleanupError = error;
    } finally {
      removeSignalCleanup();
    }
  }

  if (commandError !== undefined || cleanupError !== undefined || result?.exitCode !== 0) {
    const failures = [
      commandError,
      result?.exitCode !== undefined && result.exitCode !== 0
        ? new Error(`Docker exited with status ${result.exitCode}.`)
        : undefined,
      cleanupError,
    ].filter((error): error is NonNullable<typeof error> => error !== undefined);
    const error =
      failures.length === 1
        ? failures[0]
        : new AggregateError(
            failures,
            `Gateway/node compatibility ${params.phase} command or container cleanup failed.`,
          );
    writeGatewayNodeCompatFailureDiagnostic(
      params.outputDir,
      params.phase,
      params.logPath,
      result?.exitCode,
      error,
    );
    throw new Error(
      `Gateway/node compatibility ${params.phase} container failed; redacted bounded diagnostics were retained.`,
      error === undefined ? undefined : { cause: error },
    );
  }
  return result;
}

function installGatewayNodeCompatSignalCleanup(params: {
  containerName: string;
  logPath: string;
  outputDir: string;
  phase: "prepare" | "runtime";
}) {
  let handled = false;
  const handlers = new Map<NodeJS.Signals, () => void>();
  for (const signal of ["SIGHUP", "SIGINT", "SIGTERM"] as const) {
    const handler = () => {
      if (handled) {
        return;
      }
      handled = true;
      try {
        removeGatewayNodeCompatContainerSync(params.containerName, params.logPath);
      } catch (error) {
        writeGatewayNodeCompatFailureDiagnostic(
          params.outputDir,
          params.phase,
          params.logPath,
          undefined,
          new Error(`Container cleanup failed during ${signal}.`, { cause: error }),
        );
      }
    };
    handlers.set(signal, handler);
    // Cleanup must finish before the shared command runner handles the signal and exits.
    process.prependListener(signal, handler);
  }
  return () => {
    for (const [signal, handler] of handlers) {
      process.removeListener(signal, handler);
    }
  };
}

async function removeGatewayNodeCompatContainer(
  containerName: string,
  logPath: string,
  runCommandImpl: GatewayNodeCompatDockerCommand,
) {
  assertGatewayNodeCompatContainerName(containerName);
  writeFileSync(logPath, "", { encoding: "utf8", flag: "a", mode: 0o600 });
  try {
    await runCommandImpl("docker", gatewayNodeCompatContainerRemoveArgs(containerName), {
      logPath,
      timeoutMs: GATEWAY_NODE_COMPAT_CONTAINER_CLEANUP_TIMEOUT_MS,
      check: false,
    });
  } catch {
    // The daemon inventory below is authoritative even when the rm client is interrupted.
  }
  const inventory = await runCommandImpl("docker", gatewayNodeCompatContainerInventoryArgs(), {
    logPath,
    timeoutMs: GATEWAY_NODE_COMPAT_CONTAINER_CLEANUP_TIMEOUT_MS,
    check: false,
  });
  assertGatewayNodeCompatContainerRemoved(containerName, inventory);
}

function removeGatewayNodeCompatContainerSync(containerName: string, logPath: string) {
  assertGatewayNodeCompatContainerName(containerName);
  mkdirSync(dirname(logPath), { recursive: true });
  writeFileSync(logPath, "", { encoding: "utf8", flag: "a", mode: 0o600 });
  const removeResult = spawnSync("docker", gatewayNodeCompatContainerRemoveArgs(containerName), {
    encoding: "utf8",
    timeout: GATEWAY_NODE_COMPAT_CONTAINER_CLEANUP_TIMEOUT_MS,
    maxBuffer: GATEWAY_NODE_COMPAT_MAX_DIAGNOSTIC_BYTES,
  });
  const inventory = spawnSync("docker", gatewayNodeCompatContainerInventoryArgs(), {
    encoding: "utf8",
    timeout: GATEWAY_NODE_COMPAT_CONTAINER_CLEANUP_TIMEOUT_MS,
    maxBuffer: GATEWAY_NODE_COMPAT_MAX_DIAGNOSTIC_BYTES,
  });
  writeFileSync(
    logPath,
    boundGatewayNodeCompatDiagnostic(
      `signal cleanup rm status=${removeResult.status ?? "unavailable"} error=${
        removeResult.error ? formatError(removeResult.error) : "none"
      }\n${removeResult.stdout}${removeResult.stderr}\nsignal cleanup inventory status=${
        inventory.status ?? "unavailable"
      } error=${inventory.error ? formatError(inventory.error) : "none"}\n${inventory.stdout}${
        inventory.stderr
      }\n`,
    ),
    { encoding: "utf8", flag: "a", mode: 0o600 },
  );
  assertGatewayNodeCompatContainerRemoved(containerName, {
    exitCode: inventory.status,
    stdout: inventory.stdout,
    stderr: inventory.stderr,
    error: inventory.error,
  });
}

function gatewayNodeCompatContainerRemoveArgs(containerName: string) {
  return ["rm", "--force", containerName];
}

function gatewayNodeCompatContainerInventoryArgs() {
  return ["container", "ls", "--all", "--format", "{{.Names}}"];
}

function assertGatewayNodeCompatContainerRemoved(
  containerName: string,
  inventory: {
    error?: unknown;
    exitCode: number | null;
    stderr: string;
    stdout: string;
  },
) {
  if (inventory.error || inventory.exitCode !== 0) {
    throw new Error(
      `Failed to verify Gateway/node compatibility container cleanup: ${
        inventory.error
          ? formatError(inventory.error)
          : inventory.stderr || inventory.stdout || `exit ${inventory.exitCode ?? "unavailable"}`
      }`,
    );
  }
  if (inventory.stdout.split(/\r?\n/u).filter(Boolean).includes(containerName)) {
    throw new Error(`Gateway/node compatibility container ${containerName} still exists.`);
  }
}

function createGatewayNodeCompatContainerName(phase: "prepare" | "runtime") {
  return `openclaw-gateway-node-compat-${phase}-${process.pid}-${randomBytes(8).toString("hex")}`;
}

function assertGatewayNodeCompatContainerName(containerName: string) {
  if (!GATEWAY_NODE_COMPAT_CONTAINER_NAME_RE.test(containerName)) {
    throw new Error("Gateway/node compatibility container name is invalid.");
  }
}

function redactGatewayNodeCompatDiagnostic(value: string) {
  /* oxlint-disable eslint/no-control-regex -- Diagnostics must strip terminal control bytes. */
  return value
    .replace(/\u001b\[[0-?]*[ -/]*[@-~]/gu, "")
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/gu, "")
    .replace(
      /\b(?:gh[opusr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b/gu,
      "[REDACTED_GITHUB_TOKEN]",
    )
    .replace(/\b(authorization|token|OPENCLAW_GATEWAY_TOKEN)\s*[:=]\s*\S+/giu, "$1=[REDACTED]");
  /* oxlint-enable eslint/no-control-regex */
}

function boundGatewayNodeCompatDiagnostic(value: string) {
  const redacted = redactGatewayNodeCompatDiagnostic(value);
  const bytes = Buffer.from(redacted);
  if (bytes.byteLength <= GATEWAY_NODE_COMPAT_MAX_DIAGNOSTIC_BYTES) {
    return redacted;
  }
  const marker = "\n[diagnostic middle truncated]\n";
  const markerBytes = Buffer.byteLength(marker);
  const availableBytes = GATEWAY_NODE_COMPAT_MAX_DIAGNOSTIC_BYTES - markerBytes - 6;
  const headBytes = Math.min(8 * 1024, Math.floor(availableBytes / 2));
  const tailBytes = availableBytes - headBytes;
  return (
    bytes.subarray(0, headBytes).toString("utf8") +
    marker +
    bytes.subarray(bytes.byteLength - tailBytes).toString("utf8")
  );
}

function readGatewayNodeCompatBoundedTail(path: string, maxBytes: number) {
  const fd = openSync(path, "r");
  try {
    const size = fstatSync(fd).size;
    const length = Math.min(size, maxBytes);
    const buffer = Buffer.alloc(length);
    readSync(fd, buffer, 0, length, size - length);
    return buffer.toString("utf8");
  } finally {
    closeSync(fd);
  }
}

function writeGatewayNodeCompatContainerDiagnostic(
  phase: "prepare" | "runtime",
  error: unknown,
  logsDir: string,
  hostIdentity: ProcessIdentity,
) {
  const errorText =
    error instanceof Error ? (error.stack ?? error.message) : `Unknown error: ${String(error)}`;
  const logTails = readdirSync(logsDir)
    .filter((name) => name.endsWith(".log"))
    .toSorted()
    .map((name) => {
      const tail = readGatewayNodeCompatBoundedTail(join(logsDir, name), 8 * 1024);
      return `\n--- ${name} ---\n${tail}`;
    })
    .join("");
  const redacted = redactGatewayNodeCompatDiagnostic(`${errorText}${logTails}`);
  const bounded = Buffer.from(redacted).subarray(-GATEWAY_NODE_COMPAT_MAX_DIAGNOSTIC_BYTES);
  const diagnosticPath = join(
    GATEWAY_NODE_COMPAT_CONTAINER_DIAGNOSTICS_PATH,
    `${phase}-container.log`,
  );
  writeFileSync(diagnosticPath, bounded, { mode: 0o600 });
  chownSync(diagnosticPath, hostIdentity.uid, hostIdentity.gid);
}

export async function prepareGatewayNodeLinuxCompatContainer(plan: GatewayNodeCompatExecutionPlan) {
  assertGatewayNodeCompatPlatform();
  assertGatewayNodeCompatBaseline(plan.baseline.version);
  const { candidateIdentity, hostIdentity } = readGatewayNodeCompatContainerIdentities();
  prepareGatewayNodeCompatContainerOutput(
    GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH,
    hostIdentity,
  );
  assertGatewayNodeCompatDiagnosticsDirectory(hostIdentity);
  try {
    try {
      await withGatewayNodeCompatCleanup(async (own) => {
        const workDir = createOwnedDirectory("gateway-node-compat-prepare", own);
        const logsDir = join(workDir, "logs");
        mkdirSync(logsDir, { recursive: true, mode: 0o700 });
        try {
          const candidate = await installCompatRuntime(
            "candidate",
            plan.candidate,
            logsDir,
            own,
            candidateIdentity,
          );
          const baseline = await installCompatRuntime("baseline", plan.baseline, logsDir, own);
          await terminateGatewayNodeCompatProcesses(candidateIdentity);
          const manifest: GatewayNodeCompatPreparedManifest = {
            schema: GATEWAY_NODE_COMPAT_PREPARED_SCHEMA,
            candidate: {
              treeSha256: copyGatewayNodeCompatPreparedRuntime(
                candidate.prefixDir,
                join(GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH, "candidate"),
                hostIdentity,
              ),
            },
            baseline: {
              treeSha256: copyGatewayNodeCompatPreparedRuntime(
                baseline.prefixDir,
                join(GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH, "baseline"),
                hostIdentity,
              ),
            },
          };
          const manifestPath = join(
            GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH,
            GATEWAY_NODE_COMPAT_PREPARED_MANIFEST,
          );
          writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, {
            encoding: "utf8",
            mode: 0o600,
          });
          chownSync(manifestPath, hostIdentity.uid, hostIdentity.gid);
        } catch (error) {
          writeGatewayNodeCompatContainerDiagnostic("prepare", error, logsDir, hostIdentity);
          throw error;
        }
      });
    } finally {
      await terminateGatewayNodeCompatProcesses(candidateIdentity);
    }
    validateGatewayNodeCompatPreparedRuntime(GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH);
  } finally {
    restoreGatewayNodeCompatTreeOwnership(
      GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH,
      hostIdentity,
    );
  }
}

export async function runGatewayNodeLinuxCompatContainer(plan: GatewayNodeCompatExecutionPlan) {
  assertGatewayNodeCompatPlatform();
  assertGatewayNodeCompatBaseline(plan.baseline.version);
  const { candidateIdentity, hostIdentity } = readGatewayNodeCompatContainerIdentities();
  const preparedManifest = validateGatewayNodeCompatPreparedRuntime(
    GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH,
    hostIdentity,
  );
  prepareGatewayNodeCompatContainerOutput(plan.outputDir, hostIdentity);
  assertGatewayNodeCompatDiagnosticsDirectory(hostIdentity);
  try {
    let collected: { drafts: CaseDraft[]; tokens: string[] };
    try {
      collected = await withGatewayNodeCompatCleanup(async (own) => {
        const workDir = createOwnedDirectory("gateway-node-compat-runtime", own);
        const logsDir = join(workDir, "logs");
        mkdirSync(logsDir, { recursive: true, mode: 0o700 });
        try {
          const installedCandidate = loadGatewayNodeCompatPreparedRuntime(
            "candidate",
            plan.candidate,
            preparedManifest.candidate.treeSha256,
            own,
            candidateIdentity,
          );
          const installedBaseline = loadGatewayNodeCompatPreparedRuntime(
            "baseline",
            plan.baseline,
            preparedManifest.baseline.treeSha256,
            own,
          );
          const candidate = await runGatewayNodeCompatRuntimeLifecycle(
            "candidate",
            installedCandidate,
            plan.candidate,
            logsDir,
          );
          const baseline = await runGatewayNodeCompatRuntimeLifecycle(
            "baseline",
            installedBaseline,
            plan.baseline,
            logsDir,
          );
          const runtimes = { candidate, baseline };
          const drafts: CaseDraft[] = [];
          const tokens: string[] = [];

          for (const compatCase of buildGatewayNodeCompatCases()) {
            const token = randomBytes(32).toString("hex");
            tokens.push(token);
            drafts.push(
              await withGatewayNodeCompatCleanup((caseOwn) =>
                runCompatCase({
                  compatCase,
                  gateway: runtimes[compatCase.gateway],
                  node: runtimes[compatCase.node],
                  logsDir,
                  own: caseOwn,
                  token,
                }),
              ),
            );
            await terminateGatewayNodeCompatProcesses(candidateIdentity);
          }
          return { drafts, tokens };
        } catch (error) {
          writeGatewayNodeCompatContainerDiagnostic("runtime", error, logsDir, hostIdentity);
          throw error;
        }
      });
    } finally {
      await terminateGatewayNodeCompatProcesses(candidateIdentity);
    }
    const { drafts, tokens } = collected;

    for (const draft of drafts) {
      const gatewayAcceptedNodeMin = resolveGatewayNodeCompatAcceptedMin(
        drafts,
        draft.compatCase.gateway,
      );
      const evidence = buildGatewayNodeCompatEvidence({
        ...draft,
        gatewayAcceptedNodeMin,
        producer: plan.producer,
      });
      writeFileSync(
        join(plan.outputDir, `${evidence.caseId}.json`),
        canonicalizeGatewayNodeCompatEvidence(evidence),
        {
          encoding: "utf8",
          mode: 0o600,
        },
      );
    }
    assertGatewayNodeCompatArtifactSafe(plan.outputDir, tokens);
  } finally {
    restoreGatewayNodeCompatContainerOutputOwnership(plan.outputDir, hostIdentity);
  }
}

function assertGatewayNodeCompatPlatform() {
  if (process.platform !== "linux") {
    throw new Error(
      `Gateway/node compatibility producer requires Linux, got ${process.platform}/${process.arch}.`,
    );
  }
  resolveGatewayNodeCompatArchitecture(process.arch);
}

export function resolveGatewayNodeCompatArchitecture(
  architecture: string,
): GatewayNodeCompatArchitecture {
  if (architecture === "x64" || architecture === "arm64") {
    return architecture;
  }
  throw new Error(
    `Gateway/node compatibility producer requires x64 or arm64, got ${architecture}.`,
  );
}

function assertGatewayNodeCompatBaseline(version: string) {
  if (version !== GATEWAY_NODE_COMPAT_BASELINE_VERSION) {
    throw new Error(
      `Gateway/node compatibility baseline must be ${GATEWAY_NODE_COMPAT_BASELINE_VERSION}.`,
    );
  }
}

function assertGatewayNodeCompatPackageHash(id: RuntimeId, input: GatewayNodeCompatPackageInput) {
  if (sha256File(input.tgzPath) !== input.sha256) {
    throw new Error(`${id} package SHA-256 mismatch.`);
  }
}

export async function validateGatewayNodeCompatPackageInputs(
  params: GatewayNodeCompatRunParams,
  fetchJson: (path: string, label: string) => Promise<unknown> = (path, label) =>
    fetchActionsJson(params.actions, path, label),
) {
  const selections = [params.candidate, params.baseline] as const;
  const metadata = await Promise.all(
    selections.map((selection, index) =>
      fetchJson(
        `actions/artifacts/${selection.actionsArtifact.id}`,
        `${index === 0 ? "candidate" : "baseline"} artifact metadata`,
      ),
    ),
  );
  const producerRuns = new Map(
    selections.map((selection) => [
      `${selection.actionsArtifact.runId}:${selection.actionsArtifact.runAttempt}`,
      selection.actionsArtifact,
    ]),
  );
  const provenance = new Map(
    await Promise.all(
      [...producerRuns].map(async ([key, artifact]) => {
        const runAttempt = artifact.runAttempt;
        const runPath = `actions/runs/${artifact.runId}/attempts/${runAttempt}`;
        const [workflowRun, workflowJobs] = await Promise.all([
          fetchJson(runPath, "Actions workflow attempt"),
          fetchGatewayNodeCompatProducerJobs((page) =>
            fetchJson(
              `${runPath}/jobs?per_page=${JOBS_PAGE_SIZE}&page=${page}`,
              `Actions producer jobs page ${page}`,
            ),
          ),
        ]);
        return [key, { workflowRun, workflowJobs }] as const;
      }),
    ),
  );
  const validate = (selection: GatewayNodeCompatPackageSelection, artifactMetadata: unknown) => {
    const attempt = provenance.get(
      `${selection.actionsArtifact.runId}:${selection.actionsArtifact.runAttempt}`,
    );
    if (!attempt) {
      throw new Error("Actions artifact producer attempt metadata is missing.");
    }
    const input = validateGatewayNodeCompatArtifactBinding({
      selection,
      actions: params.actions,
      artifactMetadata,
      ...attempt,
    });
    return {
      ...input,
      tarballSizeBytes: readGatewayNodeCompatTarballSize(input.tgzPath),
    };
  };
  return [validate(selections[0], metadata[0]), validate(selections[1], metadata[1])] as const;
}

export function buildGatewayNodeCompatExecutionPlan(params: {
  candidate: GatewayNodeCompatPackageInput;
  baseline: GatewayNodeCompatPackageInput;
  producer: GatewayNodeCompatRunParams["producer"];
}): GatewayNodeCompatExecutionPlan {
  return {
    schema: GATEWAY_NODE_COMPAT_EXECUTION_PLAN_SCHEMA,
    candidate: buildGatewayNodeCompatExecutionPackage(
      params.candidate,
      GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_PATH,
    ),
    baseline: buildGatewayNodeCompatExecutionPackage(
      params.baseline,
      GATEWAY_NODE_COMPAT_CONTAINER_BASELINE_PATH,
    ),
    producer: { ...params.producer },
    outputDir: GATEWAY_NODE_COMPAT_CONTAINER_OUTPUT_PATH,
  };
}

function buildGatewayNodeCompatExecutionPackage(
  input: GatewayNodeCompatPackageInput,
  tgzPath: string,
): GatewayNodeCompatPackageExecutionInput {
  return {
    tgzPath,
    version: input.version,
    sourceSha: input.sourceSha,
    sha256: input.sha256,
    tarballSizeBytes: input.tarballSizeBytes,
    artifactFileName: basename(input.tgzPath),
    actionsArtifact: {
      id: input.actionsArtifact.id,
      name: input.actionsArtifact.name,
      digest: input.actionsArtifact.digest,
      sizeBytes: input.actionsArtifact.sizeBytes,
      runId: input.actionsArtifact.runId,
      runAttempt: input.actionsArtifact.runAttempt,
    },
  };
}

export function canonicalizeGatewayNodeCompatExecutionPlan(plan: GatewayNodeCompatExecutionPlan) {
  return `${JSON.stringify(plan, null, 2)}\n`;
}

export function readGatewayNodeCompatExecutionPlan(
  planPath: string,
): GatewayNodeCompatExecutionPlan {
  const metadata = lstatSync(planPath);
  if (
    metadata.isSymbolicLink() ||
    !metadata.isFile() ||
    metadata.size < 1 ||
    metadata.size > GATEWAY_NODE_COMPAT_MAX_PLAN_BYTES
  ) {
    throw new Error("Gateway/node compatibility execution plan must be a bounded regular file.");
  }
  const bytes = readFileSync(planPath);
  const text = decodeUtf8Exact(bytes, "Gateway/node compatibility execution plan");
  const value: unknown = JSON.parse(text);
  const plan = validateGatewayNodeCompatExecutionPlan(value);
  if (text !== canonicalizeGatewayNodeCompatExecutionPlan(plan)) {
    throw new Error("Gateway/node compatibility execution plan must be canonical JSON.");
  }
  return plan;
}

function validateGatewayNodeCompatExecutionPlan(value: unknown): GatewayNodeCompatExecutionPlan {
  const plan = requirePlanObject(value, "execution plan");
  assertPlanKeys(plan, "execution plan", [
    "schema",
    "candidate",
    "baseline",
    "producer",
    "outputDir",
  ]);
  if (plan.schema !== GATEWAY_NODE_COMPAT_EXECUTION_PLAN_SCHEMA) {
    throw new Error("Gateway/node compatibility execution plan schema is unsupported.");
  }
  if (plan.outputDir !== GATEWAY_NODE_COMPAT_CONTAINER_OUTPUT_PATH) {
    throw new Error("Gateway/node compatibility execution plan output path is invalid.");
  }
  const candidate = validateGatewayNodeCompatExecutionPackage(
    plan.candidate,
    "candidate",
    GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_PATH,
  );
  const baseline = validateGatewayNodeCompatExecutionPackage(
    plan.baseline,
    "baseline",
    GATEWAY_NODE_COMPAT_CONTAINER_BASELINE_PATH,
  );
  const producer = requirePlanObject(plan.producer, "producer");
  assertPlanKeys(producer, "producer", ["repository", "workflowSha", "runId", "runAttempt", "job"]);
  const repository = requirePlanString(producer.repository, "producer.repository", 255);
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/u.test(repository)) {
    throw new Error("producer.repository is invalid.");
  }
  return {
    schema: GATEWAY_NODE_COMPAT_EXECUTION_PLAN_SCHEMA,
    candidate,
    baseline,
    producer: {
      repository,
      workflowSha: requirePlanPattern(
        producer.workflowSha,
        "producer.workflowSha",
        /^[a-f0-9]{40}$/u,
      ),
      runId: requirePlanPattern(producer.runId, "producer.runId", /^[1-9][0-9]*$/u),
      runAttempt: requirePlanPositiveInteger(producer.runAttempt, "producer.runAttempt"),
      job: requirePlanString(producer.job, "producer.job", 255),
    },
    outputDir: GATEWAY_NODE_COMPAT_CONTAINER_OUTPUT_PATH,
  };
}

function validateGatewayNodeCompatExecutionPackage(
  value: unknown,
  label: string,
  expectedPath: string,
): GatewayNodeCompatPackageExecutionInput {
  const input = requirePlanObject(value, label);
  assertPlanKeys(input, label, [
    "tgzPath",
    "version",
    "sourceSha",
    "sha256",
    "tarballSizeBytes",
    "actionsArtifact",
    "artifactFileName",
  ]);
  if (input.tgzPath !== expectedPath) {
    throw new Error(`${label}.tgzPath is invalid.`);
  }
  const artifactFileName = requirePlanString(
    input.artifactFileName,
    `${label}.artifactFileName`,
    255,
  );
  if (
    artifactFileName === "." ||
    artifactFileName === ".." ||
    basename(artifactFileName) !== artifactFileName
  ) {
    throw new Error(`${label}.artifactFileName is invalid.`);
  }
  const actionsArtifact = requirePlanObject(input.actionsArtifact, `${label}.actionsArtifact`);
  assertPlanKeys(actionsArtifact, `${label}.actionsArtifact`, [
    "id",
    "name",
    "digest",
    "runId",
    "runAttempt",
    "sizeBytes",
  ]);
  return {
    tgzPath: expectedPath,
    version: requirePlanString(input.version, `${label}.version`, 128),
    sourceSha: requirePlanPattern(input.sourceSha, `${label}.sourceSha`, /^[a-f0-9]{40}$/u),
    sha256: requirePlanPattern(input.sha256, `${label}.sha256`, /^[a-f0-9]{64}$/u),
    tarballSizeBytes: requirePlanPositiveInteger(
      input.tarballSizeBytes,
      `${label}.tarballSizeBytes`,
    ),
    artifactFileName,
    actionsArtifact: {
      id: requirePlanPositiveInteger(actionsArtifact.id, `${label}.actionsArtifact.id`),
      name: requirePlanString(actionsArtifact.name, `${label}.actionsArtifact.name`, 255),
      digest: requirePlanPattern(
        actionsArtifact.digest,
        `${label}.actionsArtifact.digest`,
        /^sha256:[a-f0-9]{64}$/u,
      ) as `sha256:${string}`,
      sizeBytes: requirePlanPositiveInteger(
        actionsArtifact.sizeBytes,
        `${label}.actionsArtifact.sizeBytes`,
      ),
      runId: requirePlanPattern(
        actionsArtifact.runId,
        `${label}.actionsArtifact.runId`,
        /^[1-9][0-9]*$/u,
      ),
      runAttempt: requirePlanPositiveInteger(
        actionsArtifact.runAttempt,
        `${label}.actionsArtifact.runAttempt`,
      ),
    },
  };
}

export function buildGatewayNodeCompatPreparationDockerArgs(params: {
  containerName: string;
  planPath: string;
  candidateTgzPath: string;
  baselineTgzPath: string;
  diagnosticsDir: string;
  preparedDir: string;
  workflowRoot: string;
}) {
  return [
    ...buildGatewayNodeCompatDockerBaseArgs(params.workflowRoot, false, params.containerName),
    "--env",
    `NODE_OPTIONS=${GATEWAY_NODE_COMPAT_INSTALL_NODE_OPTIONS}`,
    "--mount",
    buildDockerBindMount(params.planPath, GATEWAY_NODE_COMPAT_CONTAINER_PLAN_PATH, true),
    "--mount",
    buildDockerBindMount(
      params.candidateTgzPath,
      GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_PATH,
      true,
    ),
    "--mount",
    buildDockerBindMount(params.baselineTgzPath, GATEWAY_NODE_COMPAT_CONTAINER_BASELINE_PATH, true),
    "--mount",
    buildDockerBindMount(params.preparedDir, GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH, false),
    "--mount",
    buildDockerBindMount(
      params.diagnosticsDir,
      GATEWAY_NODE_COMPAT_CONTAINER_DIAGNOSTICS_PATH,
      false,
    ),
    GATEWAY_NODE_COMPAT_CONTAINER_IMAGE,
    "node",
    "--input-type=module",
    "--eval",
    GATEWAY_NODE_COMPAT_PREPARE_ENTRYPOINT,
  ];
}

export function buildGatewayNodeCompatDockerArgs(params: {
  containerName: string;
  plan: GatewayNodeCompatExecutionPlan;
  planPath: string;
  diagnosticsDir: string;
  preparedDir: string;
  outputDir: string;
  workflowRoot: string;
}) {
  return [
    ...buildGatewayNodeCompatDockerBaseArgs(params.workflowRoot, true, params.containerName),
    "--mount",
    buildDockerBindMount(params.planPath, GATEWAY_NODE_COMPAT_CONTAINER_PLAN_PATH, true),
    "--mount",
    buildDockerBindMount(params.preparedDir, GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH, true),
    "--mount",
    buildDockerBindMount(params.outputDir, params.plan.outputDir, false),
    "--mount",
    buildDockerBindMount(
      params.diagnosticsDir,
      GATEWAY_NODE_COMPAT_CONTAINER_DIAGNOSTICS_PATH,
      false,
    ),
    GATEWAY_NODE_COMPAT_CONTAINER_IMAGE,
    "node",
    "--input-type=module",
    "--eval",
    GATEWAY_NODE_COMPAT_CONTAINER_ENTRYPOINT,
  ];
}

function buildGatewayNodeCompatDockerBaseArgs(
  workflowRoot: string,
  networkNone: boolean,
  containerName: string,
) {
  assertGatewayNodeCompatContainerName(containerName);
  const uid = process.getuid?.() ?? 1000;
  const gid = process.getgid?.() ?? 1000;
  const candidateIdentity = resolveGatewayNodeCompatCandidateIdentity(uid);
  // Docker's default PID namespace is private; --pid only enables sharing modes.
  return [
    "run",
    "--rm",
    "--name",
    containerName,
    "--init",
    "--read-only",
    "--cap-drop",
    "ALL",
    "--cap-add",
    "CHOWN",
    "--cap-add",
    "DAC_OVERRIDE",
    "--cap-add",
    "KILL",
    "--cap-add",
    "SETGID",
    "--cap-add",
    "SETUID",
    "--security-opt",
    "no-new-privileges",
    "--pids-limit",
    "512",
    "--cpus",
    "4",
    "--memory",
    "8g",
    "--memory-swap",
    "8g",
    "--log-driver",
    "none",
    "--hostname",
    "gateway-node-compat",
    ...(networkNone ? ["--network", "none"] : []),
    "--user",
    "0:0",
    "--tmpfs",
    "/tmp:rw,exec,nosuid,nodev,size=8589934592",
    "--workdir",
    "/tmp",
    "--env",
    "CI=1",
    "--env",
    "HOME=/tmp/openclaw-home",
    "--env",
    "NPM_CONFIG_CACHE=/tmp/npm-cache",
    "--env",
    "NPM_CONFIG_UPDATE_NOTIFIER=false",
    "--env",
    "NPM_CONFIG_FUND=false",
    "--env",
    "NPM_CONFIG_AUDIT=false",
    "--env",
    `${GATEWAY_NODE_COMPAT_CONTAINER_HOST_UID_ENV}=${uid}`,
    "--env",
    `${GATEWAY_NODE_COMPAT_CONTAINER_HOST_GID_ENV}=${gid}`,
    "--env",
    `${GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_UID_ENV}=${candidateIdentity.uid}`,
    "--env",
    `${GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_GID_ENV}=${candidateIdentity.gid}`,
    "--mount",
    buildDockerBindMount(join(workflowRoot, "scripts"), "/workflow/scripts", true),
    "--mount",
    buildDockerBindMount(
      join(workflowRoot, "packages", "normalization-core"),
      "/workflow/packages/normalization-core",
      true,
    ),
    "--mount",
    buildDockerBindMount(
      resolveGatewayNodeCompatTrustedWsRoot(),
      "/workflow/node_modules/ws",
      true,
    ),
  ];
}

function resolveGatewayNodeCompatTrustedWsRoot() {
  return dirname(fileURLToPath(import.meta.resolve("ws/package.json")));
}

function buildDockerBindMount(source: string, target: string, readonly: boolean) {
  const resolvedSource = resolve(source);
  if (resolvedSource.includes(",") || /[\r\n]/u.test(resolvedSource)) {
    throw new Error("Gateway/node compatibility Docker mount source is invalid.");
  }
  return `type=bind,src=${resolvedSource},dst=${target}${readonly ? ",readonly" : ""}`;
}

export function resolveGatewayNodeCompatCandidateIdentity(hostUid: number): ProcessIdentity {
  const uid =
    hostUid === GATEWAY_NODE_COMPAT_DEFAULT_CANDIDATE_UID
      ? GATEWAY_NODE_COMPAT_DEFAULT_CANDIDATE_UID - 1
      : GATEWAY_NODE_COMPAT_DEFAULT_CANDIDATE_UID;
  return { uid, gid: uid };
}

function readGatewayNodeCompatContainerIdentities() {
  if (process.getuid?.() !== 0 || process.getgid?.() !== 0) {
    throw new Error("Gateway/node compatibility container harness must run as root.");
  }
  const hostIdentity = {
    uid: readGatewayNodeCompatContainerId(GATEWAY_NODE_COMPAT_CONTAINER_HOST_UID_ENV),
    gid: readGatewayNodeCompatContainerId(GATEWAY_NODE_COMPAT_CONTAINER_HOST_GID_ENV),
  };
  const candidateIdentity = {
    uid: readGatewayNodeCompatContainerId(GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_UID_ENV),
    gid: readGatewayNodeCompatContainerId(GATEWAY_NODE_COMPAT_CONTAINER_CANDIDATE_GID_ENV),
  };
  if (
    candidateIdentity.uid === 0 ||
    candidateIdentity.gid === 0 ||
    candidateIdentity.uid === hostIdentity.uid
  ) {
    throw new Error("Gateway/node compatibility candidate identity is not isolated.");
  }
  return { candidateIdentity, hostIdentity };
}

function readGatewayNodeCompatContainerId(name: string) {
  const value = process.env[name] ?? "";
  if (!/^(?:0|[1-9][0-9]*)$/u.test(value)) {
    throw new Error(`Gateway/node compatibility container ${name} is invalid.`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed > 0x7fffffff) {
    throw new Error(`Gateway/node compatibility container ${name} is invalid.`);
  }
  return parsed;
}

export function prepareGatewayNodeCompatContainerOutput(
  outputDir: string,
  expectedIdentity: ProcessIdentity = {
    uid: process.getuid?.() ?? 0,
    gid: process.getgid?.() ?? 0,
  },
) {
  const outputMetadata = lstatSync(outputDir);
  if (
    outputMetadata.isSymbolicLink() ||
    !outputMetadata.isDirectory() ||
    outputMetadata.uid !== expectedIdentity.uid ||
    outputMetadata.gid !== expectedIdentity.gid ||
    (outputMetadata.mode & 0o777) !== 0o700 ||
    readdirSync(outputDir).length !== 0
  ) {
    throw new Error(
      "Gateway/node compatibility container output must start as an empty directory.",
    );
  }
}

function assertGatewayNodeCompatDiagnosticsDirectory(hostIdentity: ProcessIdentity) {
  const metadata = lstatSync(GATEWAY_NODE_COMPAT_CONTAINER_DIAGNOSTICS_PATH);
  if (
    metadata.isSymbolicLink() ||
    !metadata.isDirectory() ||
    metadata.uid !== hostIdentity.uid ||
    metadata.gid !== hostIdentity.gid ||
    (metadata.mode & 0o777) !== 0o700 ||
    readdirSync(GATEWAY_NODE_COMPAT_CONTAINER_DIAGNOSTICS_PATH).length !== 0
  ) {
    throw new Error("Gateway/node compatibility diagnostics directory is unsafe.");
  }
}

function restoreGatewayNodeCompatContainerOutputOwnership(
  outputDir: string,
  identity: ProcessIdentity,
) {
  for (const entry of readdirSync(outputDir, { withFileTypes: true })) {
    const filePath = join(outputDir, entry.name);
    if (!entry.isFile() || entry.isSymbolicLink()) {
      continue;
    }
    chmodSync(filePath, 0o600);
    chownSync(filePath, identity.uid, identity.gid);
  }
  chmodSync(outputDir, 0o700);
  chownSync(outputDir, identity.uid, identity.gid);
}

function restoreGatewayNodeCompatTreeOwnership(rootDir: string, identity: ProcessIdentity) {
  const paths = listGatewayNodeCompatTreePaths(rootDir).toReversed();
  for (const path of paths) {
    const metadata = lstatSync(path);
    if (!metadata.isSymbolicLink()) {
      chmodSync(path, metadata.isDirectory() ? 0o700 : metadata.mode & 0o111 ? 0o700 : 0o600);
      chownSync(path, identity.uid, identity.gid);
    }
  }
  chmodSync(rootDir, 0o700);
  chownSync(rootDir, identity.uid, identity.gid);
}

export function sealGatewayNodeCompatRuntimeTree(
  rootDir: string,
  identity: ProcessIdentity = { uid: 0, gid: 0 },
) {
  for (const path of listGatewayNodeCompatTreePaths(rootDir).toReversed()) {
    const metadata = lstatSync(path);
    if (metadata.isSymbolicLink()) {
      assertGatewayNodeCompatSafeSymlink(rootDir, path);
      continue;
    }
    if (!metadata.isDirectory() && !metadata.isFile()) {
      throw new Error("Gateway/node compatibility runtime contains a special file.");
    }
    chmodSync(path, metadata.isDirectory() || metadata.mode & 0o111 ? 0o555 : 0o444);
    chownSync(path, identity.uid, identity.gid);
  }
  chmodSync(rootDir, 0o555);
  chownSync(rootDir, identity.uid, identity.gid);
  assertGatewayNodeCompatRuntimeTreeReadOnly(rootDir, identity);
}

export function assertGatewayNodeCompatRuntimeTreeReadOnly(
  rootDir: string,
  identity: ProcessIdentity = { uid: 0, gid: 0 },
) {
  const paths = [rootDir, ...listGatewayNodeCompatTreePaths(rootDir)];
  for (const path of paths) {
    const metadata = lstatSync(path);
    if (metadata.isSymbolicLink()) {
      assertGatewayNodeCompatSafeSymlink(rootDir, path);
      continue;
    }
    const expectedMode = metadata.isDirectory() || metadata.mode & 0o111 ? 0o555 : 0o444;
    if (
      (!metadata.isDirectory() && !metadata.isFile()) ||
      metadata.uid !== identity.uid ||
      metadata.gid !== identity.gid ||
      (metadata.mode & 0o777) !== expectedMode
    ) {
      throw new Error("Gateway/node compatibility runtime tree is not sealed read-only.");
    }
  }
}

function copyGatewayNodeCompatPreparedRuntime(
  sourceDir: string,
  targetDir: string,
  hostIdentity: ProcessIdentity,
) {
  cpSync(sourceDir, targetDir, {
    recursive: true,
    dereference: false,
    errorOnExist: true,
    force: false,
  });
  restoreGatewayNodeCompatTreeOwnership(targetDir, hostIdentity);
  return hashGatewayNodeCompatRuntimeTree(targetDir);
}

export function validateGatewayNodeCompatPreparedRuntime(
  preparedDir: string,
  expectedIdentity: ProcessIdentity = {
    uid: process.getuid?.() ?? 0,
    gid: process.getgid?.() ?? 0,
  },
) {
  assertGatewayNodeCompatOwnedDirectory(preparedDir, expectedIdentity);
  const expectedEntries = ["baseline", "candidate", GATEWAY_NODE_COMPAT_PREPARED_MANIFEST];
  if (!isDeepStrictEqual(readdirSync(preparedDir).toSorted(), expectedEntries.toSorted())) {
    throw new Error("Gateway/node compatibility prepared runtime contains unexpected files.");
  }
  for (const id of ["candidate", "baseline"] as const) {
    assertGatewayNodeCompatTreeOwnership(join(preparedDir, id), expectedIdentity);
  }
  const manifestPath = join(preparedDir, GATEWAY_NODE_COMPAT_PREPARED_MANIFEST);
  const manifestMetadata = lstatSync(manifestPath);
  if (
    manifestMetadata.isSymbolicLink() ||
    !manifestMetadata.isFile() ||
    manifestMetadata.nlink !== 1 ||
    manifestMetadata.uid !== expectedIdentity.uid ||
    manifestMetadata.gid !== expectedIdentity.gid ||
    (manifestMetadata.mode & 0o777) !== 0o600 ||
    manifestMetadata.size < 1 ||
    manifestMetadata.size > GATEWAY_NODE_COMPAT_MAX_PLAN_BYTES
  ) {
    throw new Error("Gateway/node compatibility prepared manifest is unsafe.");
  }
  const text = decodeUtf8Exact(
    readFileSync(manifestPath),
    "Gateway/node compatibility prepared manifest",
  );
  const value = requirePlanObject(JSON.parse(text), "prepared manifest");
  assertPlanKeys(value, "prepared manifest", ["schema", "candidate", "baseline"]);
  if (value.schema !== GATEWAY_NODE_COMPAT_PREPARED_SCHEMA) {
    throw new Error("Gateway/node compatibility prepared manifest schema is invalid.");
  }
  const manifest = {
    schema: GATEWAY_NODE_COMPAT_PREPARED_SCHEMA,
    candidate: readGatewayNodeCompatPreparedManifestRuntime(value.candidate, "candidate"),
    baseline: readGatewayNodeCompatPreparedManifestRuntime(value.baseline, "baseline"),
  } satisfies GatewayNodeCompatPreparedManifest;
  if (text !== `${JSON.stringify(manifest, null, 2)}\n`) {
    throw new Error("Gateway/node compatibility prepared manifest is not canonical JSON.");
  }
  for (const id of ["candidate", "baseline"] as const) {
    if (hashGatewayNodeCompatRuntimeTree(join(preparedDir, id)) !== manifest[id].treeSha256) {
      throw new Error(`Gateway/node compatibility prepared ${id} runtime digest is invalid.`);
    }
  }
  return manifest;
}

function readGatewayNodeCompatPreparedManifestRuntime(value: unknown, label: RuntimeId) {
  const runtime = requirePlanObject(value, `prepared manifest ${label}`);
  assertPlanKeys(runtime, `prepared manifest ${label}`, ["treeSha256"]);
  return {
    treeSha256: requirePlanPattern(
      runtime.treeSha256,
      `prepared manifest ${label}.treeSha256`,
      /^[a-f0-9]{64}$/u,
    ),
  };
}

function assertGatewayNodeCompatOwnedDirectory(path: string, identity: ProcessIdentity) {
  const metadata = lstatSync(path);
  if (
    metadata.isSymbolicLink() ||
    !metadata.isDirectory() ||
    metadata.uid !== identity.uid ||
    metadata.gid !== identity.gid ||
    (metadata.mode & 0o777) !== 0o700
  ) {
    throw new Error("Gateway/node compatibility prepared runtime ownership is unsafe.");
  }
}

function assertGatewayNodeCompatTreeOwnership(rootDir: string, identity: ProcessIdentity) {
  assertGatewayNodeCompatOwnedDirectory(rootDir, identity);
  for (const path of listGatewayNodeCompatTreePaths(rootDir)) {
    const metadata = lstatSync(path);
    if (metadata.isSymbolicLink()) {
      assertGatewayNodeCompatSafeSymlink(rootDir, path);
      continue;
    }
    if (
      (!metadata.isDirectory() && !metadata.isFile()) ||
      metadata.uid !== identity.uid ||
      metadata.gid !== identity.gid ||
      (metadata.mode & 0o777) !==
        (metadata.isDirectory() ? 0o700 : metadata.mode & 0o111 ? 0o700 : 0o600)
    ) {
      throw new Error("Gateway/node compatibility prepared runtime tree is unsafe.");
    }
  }
}

function hashGatewayNodeCompatRuntimeTree(rootDir: string) {
  const hash = createHash("sha256");
  for (const path of listGatewayNodeCompatTreePaths(rootDir)) {
    const metadata = lstatSync(path);
    const relativePath = relative(rootDir, path);
    if (metadata.isSymbolicLink()) {
      assertGatewayNodeCompatSafeSymlink(rootDir, path);
      hash.update(`link\0${relativePath}\0${readlinkSync(path)}\0`);
    } else if (metadata.isDirectory()) {
      hash.update(`dir\0${relativePath}\0`);
    } else if (metadata.isFile()) {
      hash.update(`file\0${relativePath}\0${metadata.mode & 0o111 ? "x" : "-"}\0`);
      hash.update(readFileSync(path));
      hash.update("\0");
    } else {
      throw new Error("Gateway/node compatibility prepared runtime contains a special file.");
    }
  }
  return hash.digest("hex");
}

function assertGatewayNodeCompatSafeSymlink(rootDir: string, path: string) {
  const target = readlinkSync(path);
  const resolvedTarget = resolve(dirname(path), target);
  if (
    target.startsWith("/") ||
    (resolvedTarget !== rootDir && !resolvedTarget.startsWith(`${rootDir}${sep}`))
  ) {
    throw new Error("Gateway/node compatibility prepared runtime symlink escapes its root.");
  }
}

function listGatewayNodeCompatTreePaths(rootDir: string): string[] {
  return readdirSync(rootDir, { withFileTypes: true })
    .toSorted((left, right) => left.name.localeCompare(right.name))
    .flatMap((entry) => {
      const path = join(rootDir, entry.name);
      return entry.isDirectory() && !entry.isSymbolicLink()
        ? [path].concat(listGatewayNodeCompatTreePaths(path))
        : [path];
    });
}

async function terminateGatewayNodeCompatProcesses(identity: ProcessIdentity) {
  for (const signal of ["SIGTERM", "SIGKILL"] as const) {
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      const pids = listGatewayNodeCompatProcessIds(identity.uid);
      if (pids.length === 0) {
        return;
      }
      for (const pid of pids) {
        try {
          process.kill(pid, signal);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
            throw error;
          }
        }
      }
      await sleep(50);
    }
  }
  const remaining = listGatewayNodeCompatProcessIds(identity.uid);
  if (remaining.length > 0) {
    throw new Error(
      `Gateway/node compatibility candidate processes survived cleanup: ${remaining.join(",")}.`,
    );
  }
}

function listGatewayNodeCompatProcessIds(uid: number) {
  return readdirSync("/proc", { withFileTypes: true }).flatMap((entry) => {
    if (!entry.isDirectory() || !/^[1-9][0-9]*$/u.test(entry.name)) {
      return [];
    }
    try {
      const status = readFileSync(join("/proc", entry.name, "status"), "utf8");
      const owner = /^Uid:\s+([0-9]+)/mu.exec(status)?.[1];
      const state = /^State:\s+([A-Z])/mu.exec(status)?.[1];
      return owner === String(uid) && state !== "Z" ? [Number(entry.name)] : [];
    } catch (error) {
      if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) {
        return [];
      }
      throw error;
    }
  });
}

export function validateGatewayNodeCompatContainerEvidence(
  outputDir: string,
  plan: GatewayNodeCompatExecutionPlan,
) {
  const outputMetadata = lstatSync(outputDir);
  const expectedUid = process.getuid?.();
  const expectedGid = process.getgid?.();
  if (
    outputMetadata.isSymbolicLink() ||
    !outputMetadata.isDirectory() ||
    (expectedUid !== undefined && outputMetadata.uid !== expectedUid) ||
    (expectedGid !== undefined && outputMetadata.gid !== expectedGid) ||
    (outputMetadata.mode & 0o777) !== 0o700
  ) {
    throw new Error("Gateway/node compatibility container output must be a regular directory.");
  }
  const expectedCases = new Map(
    buildGatewayNodeCompatCases().map((compatCase) => [`${compatCase.caseId}.json`, compatCase]),
  );
  const entries = readdirSync(outputDir, { withFileTypes: true });
  if (entries.length !== expectedCases.size) {
    throw new Error("Gateway/node compatibility container output contains unexpected files.");
  }

  const canonicalEvidence = new Map<string, string>();
  for (const entry of entries) {
    const compatCase = expectedCases.get(entry.name);
    const filePath = join(outputDir, entry.name);
    const metadata = lstatSync(filePath);
    if (
      !compatCase ||
      entry.isSymbolicLink() ||
      !entry.isFile() ||
      metadata.isSymbolicLink() ||
      !metadata.isFile() ||
      metadata.nlink !== 1 ||
      (expectedUid !== undefined && metadata.uid !== expectedUid) ||
      (expectedGid !== undefined && metadata.gid !== expectedGid) ||
      (metadata.mode & 0o777) !== 0o600 ||
      metadata.size < 1 ||
      metadata.size > GATEWAY_NODE_COMPAT_MAX_EVIDENCE_BYTES
    ) {
      throw new Error("Gateway/node compatibility container output contains an unsafe file.");
    }
    const bytes = readFileSync(filePath);
    if (bytes.byteLength !== metadata.size) {
      throw new Error("Gateway/node compatibility evidence changed during validation.");
    }
    const text = decodeUtf8Exact(bytes, `Gateway/node compatibility evidence ${entry.name}`);
    const evidence = validateGatewayNodeCompatEvidence(JSON.parse(text));
    const canonical = canonicalizeGatewayNodeCompatEvidence(evidence);
    if (text !== canonical) {
      throw new Error(`Gateway/node compatibility evidence ${entry.name} is not canonical JSON.`);
    }
    assertGatewayNodeCompatEvidenceMatchesPlan(evidence, compatCase, plan);
    canonicalEvidence.set(entry.name, canonical);
    expectedCases.delete(entry.name);
  }
  if (expectedCases.size !== 0) {
    throw new Error("Gateway/node compatibility container output is incomplete.");
  }
  return canonicalEvidence;
}

function assertGatewayNodeCompatEvidenceMatchesPlan(
  evidence: GatewayNodeCompatEvidence,
  compatCase: GatewayNodeCompatCase,
  plan: GatewayNodeCompatExecutionPlan,
) {
  if (
    evidence.caseId !== compatCase.caseId ||
    evidence.direction !== compatCase.direction ||
    evidence.result.outcome !== compatCase.outcome
  ) {
    throw new Error(`Gateway/node compatibility evidence ${compatCase.caseId} case is forged.`);
  }
  const expectedProducer = {
    repository: plan.producer.repository,
    workflowPath: REUSABLE_WORKFLOW_PATH,
    workflowSha: plan.producer.workflowSha,
    runId: plan.producer.runId,
    runAttempt: plan.producer.runAttempt,
    job: plan.producer.job,
  };
  if (!isDeepStrictEqual(evidence.producer, expectedProducer)) {
    throw new Error(`Gateway/node compatibility evidence ${compatCase.caseId} producer is forged.`);
  }
  assertGatewayNodeCompatRuntimeMatchesPlan(
    evidence.gateway,
    plan[compatCase.gateway],
    `${compatCase.caseId} gateway`,
  );
  assertGatewayNodeCompatRuntimeMatchesPlan(
    evidence.node,
    plan[compatCase.node],
    `${compatCase.caseId} node`,
  );
  if (
    evidence.node.kind !== "linux" ||
    evidence.node.architecture !== resolveGatewayNodeCompatArchitecture(process.arch) ||
    evidence.node.protocolClientId !== "node-host"
  ) {
    throw new Error(`Gateway/node compatibility evidence ${compatCase.caseId} node is forged.`);
  }
}

function assertGatewayNodeCompatRuntimeMatchesPlan(
  runtime: GatewayNodeCompatRuntimeBinding,
  input: GatewayNodeCompatPackageExecutionInput,
  label: string,
) {
  const expectedPackagedArtifact = {
    version: input.version,
    sourceSha: input.sourceSha,
    name: input.artifactFileName,
    sha256: input.sha256,
    actionsArtifact: input.actionsArtifact,
  };
  if (
    !isDeepStrictEqual(runtime.packagedArtifact, expectedPackagedArtifact) ||
    runtime.installedRuntime.version !== input.version ||
    runtime.installedRuntime.sourceSha !== input.sourceSha ||
    runtime.installedRuntime.packageSha256 !== input.sha256
  ) {
    throw new Error(`Gateway/node compatibility evidence ${label} binding is forged.`);
  }
}

function requirePlanObject(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function assertPlanKeys(value: Record<string, unknown>, label: string, keys: string[]) {
  const expected = new Set(keys);
  if (
    Object.keys(value).length !== expected.size ||
    Object.keys(value).some((key) => !expected.delete(key)) ||
    expected.size !== 0
  ) {
    throw new Error(`${label} has invalid fields.`);
  }
}

function requirePlanString(value: unknown, label: string, maxLength: number) {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > maxLength ||
    value.trim() !== value ||
    Array.from(value).some((character) => {
      const codePoint = character.codePointAt(0) ?? 0;
      return codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f);
    })
  ) {
    throw new Error(`${label} must be a bounded trimmed string.`);
  }
  return value;
}

function requirePlanPattern(value: unknown, label: string, pattern: RegExp) {
  const normalized = requirePlanString(value, label, 255);
  if (!pattern.test(normalized)) {
    throw new Error(`${label} is invalid.`);
  }
  return normalized;
}

function requirePlanPositiveInteger(value: unknown, label: string) {
  if (!Number.isSafeInteger(value) || (value as number) < 1) {
    throw new Error(`${label} must be a positive safe integer.`);
  }
  return value as number;
}

function decodeUtf8Exact(bytes: Buffer, label: string) {
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new Error(`${label} is not valid UTF-8.`);
  }
  if (!Buffer.from(text, "utf8").equals(bytes)) {
    throw new Error(`${label} is not canonically encoded UTF-8.`);
  }
  return text;
}

export async function fetchGatewayNodeCompatProducerJobs(
  fetchPage: (page: number) => Promise<unknown>,
) {
  let totalCount: number | undefined;
  const jobs: unknown[] = [];
  const jobIds = new Set<number>();
  for (let page = 1; page <= MAX_JOB_PAGES; page += 1) {
    const response = asRecord(await fetchPage(page));
    const pageTotal = response.total_count;
    const pageJobs = response.jobs;
    if (
      !Number.isSafeInteger(pageTotal) ||
      (pageTotal as number) < 0 ||
      !Array.isArray(pageJobs) ||
      pageJobs.length > JOBS_PAGE_SIZE
    ) {
      throw new Error("Actions workflow jobs page is invalid.");
    }
    totalCount ??= pageTotal as number;
    if (pageTotal !== totalCount) {
      throw new Error("Actions workflow jobs total changed during pagination.");
    }
    for (const job of pageJobs) {
      const id = asRecord(job).id;
      if (!Number.isSafeInteger(id) || (id as number) < 1) {
        throw new Error("Actions workflow job id must be a positive integer.");
      }
      if (jobIds.has(id as number)) {
        throw new Error("Actions workflow jobs pagination contains a duplicate job.");
      }
      jobIds.add(id as number);
      jobs.push(job);
    }
    if (jobs.length === totalCount) {
      return { total_count: totalCount, jobs };
    }
    if (jobs.length > totalCount || pageJobs.length < JOBS_PAGE_SIZE) {
      throw new Error("Actions workflow jobs inventory is incomplete.");
    }
  }
  throw new Error("Actions workflow jobs inventory exceeded the pagination limit.");
}

export function resolveGatewayNodeCompatAcceptedMin(drafts: CaseDraft[], gateway: RuntimeId) {
  const successfulRange = drafts.find(
    (entry) =>
      entry.compatCase.gateway === gateway &&
      entry.compatCase.outcome === "passed" &&
      entry.observation.clientMin === PROVEN_GATEWAY_ACCEPTED_NODE_MIN &&
      entry.observation.clientMax === PROVEN_GATEWAY_ACCEPTED_NODE_MIN,
  );
  const rejectedRange = drafts.find(
    (entry) =>
      entry.compatCase.gateway === gateway &&
      entry.compatCase.outcome === "protocol-mismatch" &&
      entry.observation.clientMax === DISJOINT_MAX_PROTOCOL &&
      entry.mismatch?.code === "PROTOCOL_MISMATCH" &&
      entry.mismatch.clientMaxProtocol === DISJOINT_MAX_PROTOCOL,
  );
  if (!successfulRange || !rejectedRange) {
    throw new Error(
      `Gateway ${gateway} accepted-min proof requires a [${PROVEN_GATEWAY_ACCEPTED_NODE_MIN},${PROVEN_GATEWAY_ACCEPTED_NODE_MIN}] success and max-${DISJOINT_MAX_PROTOCOL} structured mismatch.`,
    );
  }
  return PROVEN_GATEWAY_ACCEPTED_NODE_MIN;
}

async function fetchActionsJson(actions: ActionsContext, path: string, label: string) {
  const response = await fetch(`${actions.apiUrl}/repos/${actions.repository}/${path}`, {
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${actions.token}`,
      "user-agent": "openclaw-gateway-node-compat",
      "x-github-api-version": "2022-11-28",
    },
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    throw new Error(`${label} returned HTTP ${response.status}.`);
  }
  const body = await readBoundedCrossOsResponseText(response, API_JSON_LIMIT);
  const value: unknown = JSON.parse(body);
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} JSON must be an object.`);
  }
  return value;
}

async function installCompatRuntime(
  id: RuntimeId,
  input: GatewayNodeCompatPackageExecutionInput,
  logsDir: string,
  own: CleanupOwner,
  processIdentity?: ProcessIdentity,
): Promise<InstalledCompatRuntime> {
  if (sha256File(input.tgzPath) !== input.sha256) {
    throw new Error(`${id} package SHA-256 mismatch.`);
  }
  const lane = createCompatLane(`gateway-node-${id}-install`, own);
  prepareCompatLaneIdentity(lane, processIdentity);
  const env = buildGatewayNodeCompatChildEnv(lane, randomBytes(32).toString("hex"));
  env.NODE_OPTIONS = GATEWAY_NODE_COMPAT_INSTALL_NODE_OPTIONS;
  const packagePath = processIdentity
    ? stageCompatPackage(input, lane, processIdentity)
    : input.tgzPath;
  await installTarballPackage({
    lane,
    env,
    tgzPath: packagePath,
    logPath: join(logsDir, `install-${id}.log`),
    ignoreScripts: true,
    restoreBundledPluginPostinstall: false,
    processIdentity,
  });
  if (processIdentity) {
    await terminateGatewayNodeCompatProcesses(processIdentity);
  }
  const installed = readInstalledMetadata(lane.prefixDir);
  if (installed.version !== input.version || installed.commit !== input.sourceSha) {
    throw new Error(
      `${id} installed runtime identity mismatch: version=${installed.version || "<missing>"} commit=${installed.commit || "<missing>"}.`,
    );
  }
  const entryPath = installedEntryPath(lane.prefixDir);
  const packageRoot = dirname(entryPath);
  return {
    lane,
    prefixDir: lane.prefixDir,
    packageRoot,
    cliPath: join(binDirForPrefix(lane.prefixDir), "openclaw"),
    processIdentity,
    binding: {
      packagedArtifact: {
        version: input.version,
        sourceSha: input.sourceSha,
        name: input.artifactFileName,
        sha256: input.sha256,
        actionsArtifact: input.actionsArtifact,
      },
      installedRuntime: {
        version: installed.version,
        sourceSha: installed.commit,
        packageSha256: input.sha256,
        identitySha256: sha256File(entryPath),
      },
    },
  };
}

function loadGatewayNodeCompatPreparedRuntime(
  id: RuntimeId,
  input: GatewayNodeCompatPackageExecutionInput,
  treeSha256: string,
  own: CleanupOwner,
  processIdentity?: ProcessIdentity,
): InstalledCompatRuntime {
  const sourcePrefix = join(GATEWAY_NODE_COMPAT_CONTAINER_PREPARED_PATH, id);
  if (hashGatewayNodeCompatRuntimeTree(sourcePrefix) !== treeSha256) {
    throw new Error(`Gateway/node compatibility prepared ${id} runtime changed before execution.`);
  }
  const lane = createCompatLane(`gateway-node-${id}-runtime`, own);
  rmSync(lane.prefixDir, { recursive: true, force: true });
  cpSync(sourcePrefix, lane.prefixDir, {
    recursive: true,
    dereference: false,
    errorOnExist: true,
    force: false,
  });
  if (hashGatewayNodeCompatRuntimeTree(lane.prefixDir) !== treeSha256) {
    throw new Error(`Gateway/node compatibility prepared ${id} runtime copy is inconsistent.`);
  }
  if (processIdentity) {
    restoreGatewayNodeCompatTreeOwnership(lane.rootDir, processIdentity);
  }
  const installed = readInstalledMetadata(lane.prefixDir);
  if (installed.version !== input.version || installed.commit !== input.sourceSha) {
    throw new Error(
      `${id} prepared runtime identity mismatch: version=${installed.version || "<missing>"} commit=${installed.commit || "<missing>"}.`,
    );
  }
  const entryPath = installedEntryPath(lane.prefixDir);
  return {
    lane,
    prefixDir: lane.prefixDir,
    packageRoot: dirname(entryPath),
    cliPath: join(binDirForPrefix(lane.prefixDir), "openclaw"),
    processIdentity,
    binding: {
      packagedArtifact: {
        version: input.version,
        sourceSha: input.sourceSha,
        name: input.artifactFileName,
        sha256: input.sha256,
        actionsArtifact: input.actionsArtifact,
      },
      installedRuntime: {
        version: installed.version,
        sourceSha: installed.commit,
        packageSha256: input.sha256,
        identitySha256: treeSha256,
      },
    },
  };
}

async function runGatewayNodeCompatRuntimeLifecycle(
  id: RuntimeId,
  runtime: InstalledCompatRuntime,
  input: GatewayNodeCompatPackageExecutionInput,
  logsDir: string,
): Promise<InstalledCompatRuntime> {
  const env = {
    ...buildGatewayNodeCompatChildEnv(
      runtime.lane,
      randomBytes(32).toString("hex"),
      runtime.prefixDir,
    ),
    npm_config_global: "true",
    npm_config_location: "global",
    npm_config_prefix: runtime.prefixDir,
  };
  await runCommand(
    npmCommand(),
    ["rebuild", "--global", "--foreground-scripts", "--prefix", runtime.prefixDir],
    {
      cwd: runtime.lane.homeDir,
      env,
      logPath: join(logsDir, `lifecycle-${id}.log`),
      timeoutMs: 10 * 60_000,
      processIdentity: runtime.processIdentity,
    },
  );
  if (runtime.processIdentity) {
    await terminateGatewayNodeCompatProcesses(runtime.processIdentity);
  }
  const installed = readInstalledMetadata(runtime.prefixDir);
  if (installed.version !== input.version || installed.commit !== input.sourceSha) {
    throw new Error(
      `${id} post-lifecycle runtime identity mismatch: version=${installed.version || "<missing>"} commit=${installed.commit || "<missing>"}.`,
    );
  }
  // Candidate code runs under an unprivileged identity, while the finalized
  // package tree is root-owned and read-only before its identity is recorded.
  sealGatewayNodeCompatRuntimeTree(runtime.prefixDir);
  return {
    ...runtime,
    binding: {
      ...runtime.binding,
      installedRuntime: {
        version: installed.version,
        sourceSha: installed.commit,
        packageSha256: input.sha256,
        identitySha256: hashGatewayNodeCompatRuntimeTree(runtime.prefixDir),
      },
    },
  };
}

async function runCompatCase(params: CaseRunParams): Promise<CaseDraft> {
  const startedAt = new Date().toISOString();
  return withObservedGateway(
    params,
    async ({ gatewayEnv, gatewayHome, gatewayUrl, proxy, proxyPort }) => {
      const base = {
        compatCase: params.compatCase,
        gateway: params.gateway.binding,
        node: params.node.binding,
        startedAt,
      };
      if (params.compatCase.outcome === "protocol-mismatch") {
        const clientLane = createCompatLane(`${params.compatCase.caseId}-client`, params.own);
        prepareCompatLaneIdentity(clientLane, params.node.processIdentity);
        await runDisjointPackagedClient({
          runtime: params.node,
          gatewayUrl: `ws://127.0.0.1:${proxyPort}`,
          cwd: clientLane.homeDir,
          env: buildGatewayNodeCompatChildEnv(clientLane, params.token, params.node.prefixDir),
          logPath: join(params.logsDir, `${params.compatCase.caseId}-client.log`),
        });
        const observation = proxy.read();
        const mismatch = normalizeProtocolMismatch({
          legacyGatewayVersion:
            params.compatCase.gateway === "baseline"
              ? params.gateway.binding.packagedArtifact.version
              : undefined,
          observation,
        });
        return {
          ...base,
          observation: validateGatewayNodeCompatObservation({
            outcome: "protocol-mismatch",
            observation,
            mismatch,
          }),
          mismatch,
          completedAt: new Date().toISOString(),
        };
      }
      const nodeLane = createCompatLane(`${params.compatCase.caseId}-node`, params.own);
      prepareCompatLaneIdentity(nodeLane, params.node.processIdentity);
      const node = startCompatProcess({
        runtime: params.node,
        args: buildGatewayNodeCompatNodeArgs(proxyPort, params.compatCase.caseId),
        cwd: nodeLane.homeDir,
        env: buildGatewayNodeCompatChildEnv(nodeLane, params.token, params.node.prefixDir),
        logPath: join(params.logsDir, `${params.compatCase.caseId}-node.log`),
        own: params.own,
      });
      const operation = await approveAndInvokeNode({
        runtime: params.gateway,
        gatewayUrl,
        expectedNodeId: params.compatCase.caseId,
        expectedDisplayName: params.compatCase.caseId,
        env: gatewayEnv,
        cwd: gatewayHome,
        logsDir: params.logsDir,
        child: node.child,
      });
      return {
        ...base,
        observation: validateGatewayNodeCompatObservation({
          outcome: "passed",
          observation: proxy.read(),
        }),
        operation,
        startedAt,
        completedAt: new Date().toISOString(),
      };
    },
  );
}

async function withObservedGateway<T>(
  params: CaseRunParams,
  run: (context: {
    gatewayEnv: NodeJS.ProcessEnv;
    gatewayHome: string;
    gatewayUrl: string;
    proxy: Awaited<ReturnType<typeof startProtocolObserver>>;
    proxyPort: number;
  }) => Promise<T>,
) {
  const gatewayLane = createCompatLane(`${params.compatCase.caseId}-gateway`, params.own);
  const proxyLane = createCompatLane(`${params.compatCase.caseId}-proxy`, params.own);
  prepareCompatLaneIdentity(gatewayLane, params.gateway.processIdentity);
  const gatewayEnv = buildGatewayNodeCompatChildEnv(
    gatewayLane,
    params.token,
    params.gateway.prefixDir,
  );
  return withAllocatedGatewayPort(gatewayLane, async () => {
    const gateway = startCompatProcess({
      runtime: params.gateway,
      args: buildGatewayNodeCompatGatewayArgs(gatewayLane.gatewayPort),
      cwd: gatewayLane.homeDir,
      env: gatewayEnv,
      logPath: join(params.logsDir, `${params.compatCase.caseId}-gateway.log`),
      own: params.own,
    });
    await waitForGatewayPort(gatewayLane.gatewayPort, gateway.child);
    return withAllocatedGatewayPort(proxyLane, async () => {
      const gatewayUrl = `ws://127.0.0.1:${gatewayLane.gatewayPort}`;
      const proxy = await startProtocolObserver({
        port: proxyLane.gatewayPort,
        upstreamUrl: gatewayUrl,
        own: params.own,
      });
      return run({
        gatewayEnv,
        gatewayHome: gatewayLane.homeDir,
        gatewayUrl,
        proxy,
        proxyPort: proxyLane.gatewayPort,
      });
    });
  });
}

export function validateGatewayNodeCompatObservation(params: {
  outcome: Outcome;
  observation: ProtocolObservation;
  mismatch?: ProtocolMismatch;
}): ProtocolObservation {
  const { clientMin, clientMax, helloProtocol } = params.observation;
  if (
    !Number.isSafeInteger(clientMin) ||
    clientMin < 1 ||
    !Number.isSafeInteger(clientMax) ||
    clientMax < clientMin
  ) {
    throw new Error("Observed node connect frame has an invalid protocol range.");
  }
  if (params.outcome === "passed") {
    if (
      !Number.isSafeInteger(helloProtocol) ||
      helloProtocol === null ||
      helloProtocol < 1 ||
      params.observation.protocolError != null
    ) {
      throw new Error("Observed Gateway hello protocol must be a positive integer.");
    }
    return params.observation;
  }
  const mismatch = params.mismatch;
  if (
    helloProtocol !== null ||
    params.observation.protocolError == null ||
    mismatch?.code !== "PROTOCOL_MISMATCH" ||
    mismatch.clientMinProtocol !== clientMin ||
    mismatch.clientMaxProtocol !== clientMax ||
    mismatch.expectedProtocol < 1
  ) {
    throw new Error("Observed Gateway protocol mismatch does not match the node connect frame.");
  }
  return params.observation;
}

export function buildGatewayNodeCompatEvidence(
  params: CaseDraft & {
    gatewayAcceptedNodeMin: number;
    producer: GatewayNodeCompatRunParams["producer"];
  },
): GatewayNodeCompatEvidence {
  const identity = validateGatewayNodeCompatConnectIdentity(params.observation.identity);
  const passed = params.compatCase.outcome === "passed";
  const gatewayProtocolVersion = passed
    ? params.observation.helloProtocol
    : params.mismatch?.expectedProtocol;
  if (!gatewayProtocolVersion) {
    throw new Error(`Missing observed Gateway protocol for ${params.compatCase.caseId}.`);
  }
  if (passed && !params.operation) {
    throw new Error(`Passed compatibility case ${params.compatCase.caseId} is incomplete.`);
  }
  const protocol = {
    gatewayProtocolVersion,
    gatewayAcceptedNodeMin: params.gatewayAcceptedNodeMin,
    protocolClientAdvertisedMin: params.observation.clientMin,
    protocolClientAdvertisedMax: params.observation.clientMax,
    helloProtocol: passed ? params.observation.helloProtocol : null,
  };
  return {
    schema: SCHEMA,
    caseId: params.compatCase.caseId,
    direction: params.compatCase.direction,
    connection: {
      transport: "gateway-websocket",
      role: identity.role,
      mode: identity.mode,
    },
    gateway: params.gateway,
    node: {
      kind: identity.platform,
      architecture: identity.architecture,
      protocolClientId: identity.clientId,
      ...params.node,
    },
    protocol,
    producer: {
      repository: params.producer.repository,
      workflowPath: REUSABLE_WORKFLOW_PATH,
      workflowSha: params.producer.workflowSha,
      runId: params.producer.runId,
      runAttempt: params.producer.runAttempt,
      job: params.producer.job,
    },
    operation: passed ? params.operation : null,
    result: passed
      ? { outcome: "passed", startedAt: params.startedAt, completedAt: params.completedAt }
      : {
          outcome: "protocol-mismatch",
          failureCode: "PROTOCOL_MISMATCH",
          failurePhase: "connect",
          startedAt: params.startedAt,
          completedAt: params.completedAt,
        },
  } as GatewayNodeCompatEvidence;
}

export function selectExpectedPendingNodeRequest(
  pending: PendingNodeRequest[],
  expectedNodeId: string,
  expectedDisplayName: string,
) {
  const matches = pending.filter(
    (request) => request.nodeId === expectedNodeId && request.displayName === expectedDisplayName,
  );
  if (matches.length > 1) {
    throw new Error(`Multiple pending requests matched node ${expectedNodeId}.`);
  }
  const requestId = matches[0]?.requestId;
  return typeof requestId === "string" && requestId ? requestId : null;
}

async function approveAndInvokeNode(params: {
  runtime: InstalledCompatRuntime;
  gatewayUrl: string;
  expectedNodeId: string;
  expectedDisplayName: string;
  env: NodeJS.ProcessEnv;
  cwd: string;
  logsDir: string;
  child: ChildProcess;
}) {
  const deadline = Date.now() + TIMEOUT_MS;
  while (Date.now() < deadline) {
    assertChildAlive(params.child, "node");
    const pending = await runCompatCliJson<PendingNodeRequest[]>({
      runtime: params.runtime,
      args: ["nodes", "pending", "--json", "--url", params.gatewayUrl],
      env: params.env,
      cwd: params.cwd,
      logPath: join(params.logsDir, `${params.expectedNodeId}-pending.log`),
      check: false,
    });
    const requestId = selectExpectedPendingNodeRequest(
      pending ?? [],
      params.expectedNodeId,
      params.expectedDisplayName,
    );
    if (requestId) {
      await runCompatCliJson({
        runtime: params.runtime,
        args: ["nodes", "approve", requestId, "--json", "--url", params.gatewayUrl],
        env: params.env,
        cwd: params.cwd,
        logPath: join(params.logsDir, `${params.expectedNodeId}-approve.log`),
      });
    }
    const result = await runCompatCliJson<{
      ok?: unknown;
      command?: unknown;
      payload?: unknown;
    }>({
      runtime: params.runtime,
      args: buildGatewayNodeCompatInvokeArgs({
        gatewayUrl: params.gatewayUrl,
        nodeId: params.expectedNodeId,
      }),
      env: params.env,
      cwd: params.cwd,
      logPath: join(params.logsDir, `${params.expectedNodeId}-invoke.log`),
      check: false,
    });
    const nodePath = asRecord(asRecord(result?.payload).bins)[BIN];
    if (result?.ok === true && result.command === "system.which" && typeof nodePath === "string") {
      return {
        method: "node.invoke",
        command: "system.which",
        params: { bins: [BIN] },
        ok: true,
        result: { bins: { [BIN]: nodePath } },
      } satisfies GatewayNodeCompatOperation;
    }
    await sleep(1_000);
  }
  throw new Error(`Timed out invoking approved node ${params.expectedNodeId}.`);
}

async function runCompatCliJson<T>(params: {
  runtime: InstalledCompatRuntime;
  args: string[];
  env: NodeJS.ProcessEnv;
  cwd: string;
  logPath: string;
  check?: boolean;
}): Promise<T | null> {
  const invocation = resolveInstalledCliInvocation(params.runtime.cliPath, params.args, {
    env: params.env,
  });
  const result = await runCommandInvocation(invocation, {
    cwd: params.cwd,
    env: params.env,
    logPath: params.logPath,
    timeoutMs: 30_000,
    check: params.check ?? true,
    processIdentity: params.runtime.processIdentity,
  });
  if (result.exitCode !== 0 || !result.stdout.trim()) {
    return null;
  }
  return JSON.parse(result.stdout) as T;
}

async function runDisjointPackagedClient(params: {
  runtime: InstalledCompatRuntime;
  gatewayUrl: string;
  cwd: string;
  env: NodeJS.ProcessEnv;
  logPath: string;
}) {
  const runtimePath = join(params.runtime.packageRoot, "dist", "plugin-sdk", "gateway-runtime.js");
  const scriptPath = join(params.cwd, "gateway-node-disjoint-client.mjs");
  writeFileSync(
    scriptPath,
    buildDisjointPackagedClientScript({
      gatewayRuntimeUrl: pathToFileURL(runtimePath).href,
      gatewayUrl: params.gatewayUrl,
    }),
    { encoding: "utf8", mode: 0o600 },
  );
  if (params.runtime.processIdentity) {
    chownSync(scriptPath, params.runtime.processIdentity.uid, params.runtime.processIdentity.gid);
  }
  try {
    await runCommand(process.execPath, [scriptPath], {
      env: params.env,
      cwd: params.cwd,
      logPath: params.logPath,
      timeoutMs: 30_000,
      processIdentity: params.runtime.processIdentity,
    });
  } finally {
    rmSync(scriptPath, { force: true });
  }
}

export function buildDisjointPackagedClientScript(params: {
  gatewayRuntimeUrl: string;
  gatewayUrl: string;
}) {
  return `
const { GatewayClient } = await import(${JSON.stringify(params.gatewayRuntimeUrl)});
const token = process.env.OPENCLAW_GATEWAY_TOKEN;
if (!token) throw new Error("missing OPENCLAW_GATEWAY_TOKEN");
const timeout = setTimeout(() => process.exit(1), 15000); let settled = false;
const client = new GatewayClient({
  url: ${JSON.stringify(params.gatewayUrl)}, token,
  clientName: "node-host", clientVersion: "gateway-node-compat-disjoint",
  platform: "linux", mode: "node", role: "node",
  scopes: [], caps: [], commands: ["system.which"],
  minProtocol: ${DISJOINT_MIN_PROTOCOL}, maxProtocol: ${DISJOINT_MAX_PROTOCOL},
  onConnectError: () => {
    if (settled) return;
    settled = true; clearTimeout(timeout); client.stop(); process.exit(0);
  },
  onHelloOk: () => process.exit(1),
});
client.start();
`.trimStart();
}

export function normalizeProtocolMismatch(params: {
  legacyGatewayVersion?: string;
  observation: ProtocolObservation;
}): ProtocolMismatch {
  const outer = asRecord(asRecord(params.observation.protocolError).details);
  const details = Object.hasOwn(outer, "code") ? outer : asRecord(outer.details);
  if (
    params.legacyGatewayVersion === GATEWAY_NODE_COMPAT_BASELINE_VERSION &&
    Object.keys(outer).length === 1 &&
    Number.isSafeInteger(outer.expectedProtocol) &&
    (outer.expectedProtocol as number) > 0 &&
    params.observation.clientMin === DISJOINT_MIN_PROTOCOL &&
    params.observation.clientMax === DISJOINT_MAX_PROTOCOL &&
    params.observation.helloProtocol === null
  ) {
    return {
      code: "PROTOCOL_MISMATCH",
      clientMinProtocol: params.observation.clientMin,
      clientMaxProtocol: params.observation.clientMax,
      expectedProtocol: outer.expectedProtocol as number,
    };
  }
  if (
    details.code !== "PROTOCOL_MISMATCH" ||
    !Number.isSafeInteger(details.clientMinProtocol) ||
    !Number.isSafeInteger(details.clientMaxProtocol) ||
    !Number.isSafeInteger(details.expectedProtocol)
  ) {
    throw new Error(`Packaged client did not return structured PROTOCOL_MISMATCH.`);
  }
  return {
    code: "PROTOCOL_MISMATCH",
    clientMinProtocol: details.clientMinProtocol as number,
    clientMaxProtocol: details.clientMaxProtocol as number,
    expectedProtocol: details.expectedProtocol as number,
  };
}

export async function startProtocolObserver(params: {
  port: number;
  upstreamUrl: string;
  own: CleanupOwner;
}) {
  const { WebSocket, WebSocketServer } = await import("ws");
  const server = new WebSocketServer({ host: "127.0.0.1", port: params.port });
  let range: { min: number; max: number } | undefined;
  let identity: ObservedConnectIdentity | undefined;
  let helloProtocol: number | null = null;
  let protocolError: unknown | null = null;
  let inconsistent = false;
  params.own(async () => {
    for (const socket of server.clients) {
      socket.terminate();
    }
    await new Promise<void>((resolvePromise) => {
      server.close(() => resolvePromise());
    });
  });
  server.on("connection", (downstream) => {
    const upstream = new WebSocket(params.upstreamUrl);
    const pending: Array<{ data: RawData; isBinary: boolean }> = [];
    let connectId = "";
    downstream.on("message", (data, isBinary) => {
      const frame = parseJsonFrame(data);
      if (frame.method === "connect" && typeof frame.id === "string") {
        const connect = asRecord(frame.params);
        const client = asRecord(connect.client);
        if (
          Number.isSafeInteger(connect.minProtocol) &&
          Number.isSafeInteger(connect.maxProtocol) &&
          typeof connect.role === "string" &&
          typeof client.id === "string" &&
          typeof client.mode === "string" &&
          typeof client.platform === "string"
        ) {
          const next = { min: connect.minProtocol as number, max: connect.maxProtocol as number };
          inconsistent ||= Boolean(range && (range.min !== next.min || range.max !== next.max));
          range = next;
          const nextIdentity = {
            architecture: resolveGatewayNodeCompatArchitecture(process.arch),
            clientId: client.id,
            mode: client.mode,
            platform: client.platform,
            role: connect.role,
          };
          inconsistent ||= Boolean(identity && !isDeepStrictEqual(identity, nextIdentity));
          identity = nextIdentity;
          if (connectId !== frame.id) {
            helloProtocol = null;
            protocolError = null;
          }
          connectId = frame.id;
        }
      }
      if (upstream.readyState === WebSocket.OPEN) {
        upstream.send(data, { binary: isBinary });
      } else {
        pending.push({ data, isBinary });
      }
    });
    upstream.on("open", () => {
      for (const message of pending.splice(0)) {
        upstream.send(message.data, { binary: message.isBinary });
      }
    });
    upstream.on("message", (data, isBinary) => {
      const frame = parseJsonFrame(data);
      if (frame.id === connectId) {
        const payload = asRecord(frame.payload);
        if (payload.type === "hello-ok" && Number.isSafeInteger(payload.protocol)) {
          const next = payload.protocol as number;
          inconsistent ||= helloProtocol !== null && helloProtocol !== next;
          helloProtocol = next;
          protocolError = null;
        } else if (Object.hasOwn(frame, "error")) {
          const next = frame.error;
          inconsistent ||= protocolError !== null && !isDeepStrictEqual(protocolError, next);
          protocolError = next;
          helloProtocol = null;
        }
      }
      if (downstream.readyState === WebSocket.OPEN) {
        downstream.send(data, { binary: isBinary });
      }
    });
    upstream.on("close", (code, reason) => {
      if (downstream.readyState === WebSocket.OPEN) {
        downstream.close(code, reason.toString());
      }
    });
    upstream.on("error", () => downstream.terminate());
    downstream.on("close", () => upstream.close());
  });
  await new Promise<void>((resolvePromise, rejectPromise) => {
    server.on("listening", () => resolvePromise());
    server.on("error", rejectPromise);
  });
  return {
    read(): ProtocolObservation {
      if (!range || !identity || inconsistent) {
        throw new Error("Protocol observer did not capture one consistent node session.");
      }
      return {
        clientMin: range.min,
        clientMax: range.max,
        helloProtocol,
        identity,
        protocolError,
      };
    },
  };
}

function validateGatewayNodeCompatConnectIdentity(
  value: ObservedConnectIdentity | undefined,
): ObservedConnectIdentity & {
  clientId: "node-host";
  mode: "node";
  platform: "linux";
  role: "node";
} {
  if (
    value?.role !== "node" ||
    value.mode !== "node" ||
    value.clientId !== "node-host" ||
    value.platform !== "linux"
  ) {
    throw new Error("Observed Gateway connect identity is not a Linux node-host session.");
  }
  return value as ObservedConnectIdentity & {
    clientId: "node-host";
    mode: "node";
    platform: "linux";
    role: "node";
  };
}

function parseJsonFrame(data: unknown) {
  try {
    const value: unknown = JSON.parse(
      Buffer.isBuffer(data)
        ? data.toString("utf8")
        : Array.isArray(data)
          ? Buffer.concat(data.map((part) => Buffer.from(part))).toString("utf8")
          : Buffer.from(data as Uint8Array).toString("utf8"),
    );
    return asRecord(value);
  } catch {
    return {};
  }
}

export function assertGatewayNodeCompatArtifactSafe(outputDir: string, tokens: string[]) {
  const expected = new Set(buildGatewayNodeCompatCases().map((entry) => `${entry.caseId}.json`));
  const files = readdirSync(outputDir, { recursive: true, encoding: "utf8" }).map(
    (relativePath) => ({ path: join(outputDir, relativePath), relativePath }),
  );
  if (
    files.length !== expected.size ||
    files.some(
      (file) =>
        lstatSync(file.path).isSymbolicLink() ||
        !lstatSync(file.path).isFile() ||
        !expected.delete(file.relativePath),
    ) ||
    expected.size !== 0
  ) {
    throw new Error("Gateway/node compatibility artifact contains unexpected files.");
  }
  // oxlint-disable-next-line unicorn/prefer-set-has -- Secret scanning requires byte-substring matching, not whole-buffer identity.
  const bytes = Buffer.concat(files.map((file) => readFileSync(file.path)));
  for (const token of tokens) {
    if (token && bytes.includes(Buffer.from(token))) {
      throw new Error("Gateway token leaked into compatibility artifact.");
    }
  }
}

export async function withGatewayNodeCompatCleanup<T>(
  run: (own: CleanupOwner) => Promise<T>,
): Promise<T> {
  const cleanup: Cleanup[] = [];
  const failures: unknown[] = [];
  let outcome: { ok: true; value: T } | { ok: false; error: unknown };
  try {
    outcome = {
      ok: true,
      value: await run((entry) =>
        cleanup.push(async () => {
          try {
            await entry();
          } catch (error) {
            failures.push(error);
            throw error;
          }
        }),
      ),
    };
  } catch (error) {
    outcome = { ok: false, error };
  }
  await runCleanup(cleanup);
  if (!outcome.ok) {
    throw outcome.error;
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, "Gateway/node compatibility cleanup failed.");
  }
  return outcome.value;
}

function createOwnedDirectory(name: string, own: CleanupOwner) {
  const rootDir = mkdtempSync(join(tmpdir(), `openclaw-${name}-`));
  own(() => rmSync(rootDir, { recursive: true, force: true }));
  return rootDir;
}

function createCompatLane(name: string, own: CleanupOwner): LaneState {
  const rootDir = createOwnedDirectory(name, own);
  const prefixDir = join(rootDir, "prefix");
  const homeDir = join(rootDir, "home");
  const stateDir = join(homeDir, ".openclaw");
  mkdirSync(prefixDir, { recursive: true });
  mkdirSync(stateDir, { recursive: true });
  return {
    name,
    rootDir,
    prefixDir,
    homeDir,
    stateDir,
    appDataDir: stateDir,
    gatewayPort: 0,
    phaseTimings: [],
  };
}

function prepareCompatLaneIdentity(lane: LaneState, processIdentity: ProcessIdentity | undefined) {
  if (!processIdentity) {
    return;
  }
  for (const directory of [lane.rootDir, lane.prefixDir, lane.homeDir, lane.stateDir]) {
    chmodSync(directory, 0o700);
    chownSync(directory, processIdentity.uid, processIdentity.gid);
  }
}

function stageCompatPackage(
  input: GatewayNodeCompatPackageExecutionInput,
  lane: LaneState,
  processIdentity: ProcessIdentity,
) {
  const stagedPath = join(lane.rootDir, "package.tgz");
  stageGatewayNodeCompatPackageFile(input, stagedPath);
  chownSync(stagedPath, processIdentity.uid, processIdentity.gid);
  return stagedPath;
}

export function stageGatewayNodeCompatPackageFile(
  input: GatewayNodeCompatPackageExecutionInput,
  stagedPath: string,
) {
  const sourceMetadata = lstatSync(input.tgzPath);
  if (
    sourceMetadata.isSymbolicLink() ||
    !sourceMetadata.isFile() ||
    sourceMetadata.size !== input.tarballSizeBytes
  ) {
    throw new Error(
      "Gateway/node compatibility package must match its declared regular file size.",
    );
  }

  const sourceFd = openSync(input.tgzPath, "r");
  let stagedFd: number | undefined;
  let copyError: unknown;
  try {
    stagedFd = openSync(stagedPath, "wx", 0o600);
    const buffer = Buffer.allocUnsafe(64 * 1024);
    let copiedBytes = 0;
    while (copiedBytes < sourceMetadata.size) {
      const bytesRead = readSync(
        sourceFd,
        buffer,
        0,
        Math.min(buffer.byteLength, sourceMetadata.size - copiedBytes),
        copiedBytes,
      );
      if (bytesRead === 0) {
        throw new Error("Gateway/node compatibility package ended before its declared size.");
      }
      let writtenBytes = 0;
      while (writtenBytes < bytesRead) {
        const bytesWritten = writeSync(
          stagedFd,
          buffer,
          writtenBytes,
          bytesRead - writtenBytes,
          copiedBytes + writtenBytes,
        );
        if (bytesWritten === 0) {
          throw new Error("Staged Gateway/node compatibility package write made no progress.");
        }
        writtenBytes += bytesWritten;
      }
      copiedBytes += bytesRead;
    }
  } catch (error) {
    copyError = error;
  } finally {
    closeSync(sourceFd);
    if (stagedFd !== undefined) {
      closeSync(stagedFd);
    }
  }
  if (copyError !== undefined) {
    rmSync(stagedPath, { force: true });
    throw copyError instanceof Error
      ? copyError
      : new Error("Gateway/node compatibility package copy failed.", { cause: copyError });
  }

  const stagedMetadata = lstatSync(stagedPath);
  if (!stagedMetadata.isFile() || stagedMetadata.size !== input.tarballSizeBytes) {
    rmSync(stagedPath, { force: true });
    throw new Error("Staged Gateway/node compatibility package size mismatch.");
  }
  if (sha256File(stagedPath) !== input.sha256) {
    rmSync(stagedPath, { force: true });
    throw new Error("Staged Gateway/node compatibility package SHA-256 mismatch.");
  }
  chmodSync(stagedPath, 0o400);
}

function readGatewayNodeCompatTarballSize(path: string) {
  const metadata = lstatSync(path);
  if (metadata.isSymbolicLink() || !metadata.isFile() || metadata.size < 1) {
    throw new Error("Gateway/node compatibility tarball must be a non-empty regular file.");
  }
  return metadata.size;
}

export function buildGatewayNodeCompatChildEnv(
  lane: LaneState,
  token: string,
  prefixDir = lane.prefixDir,
  inheritedEnv = process.env,
) {
  const env: NodeJS.ProcessEnv = {
    LANG: inheritedEnv.LANG ?? "C.UTF-8",
    LC_ALL: inheritedEnv.LC_ALL ?? inheritedEnv.LANG ?? "C.UTF-8",
    HOME: lane.homeDir,
    USERPROFILE: lane.homeDir,
    OPENCLAW_HOME: lane.homeDir,
    OPENCLAW_STATE_DIR: lane.stateDir,
    OPENCLAW_CONFIG_PATH: join(lane.stateDir, "openclaw.json"),
    OPENCLAW_GATEWAY_TOKEN: token,
    OPENCLAW_DISABLE_BONJOUR: "1",
    OPENCLAW_DISABLE_BUNDLED_PLUGIN_POSTINSTALL: "1",
    OPENCLAW_NO_ONBOARD: "1",
    OPENCLAW_NO_PROMPT: "1",
    CI: "1",
    NPM_CONFIG_PREFIX: prefixDir,
    PATH: `${binDirForPrefix(prefixDir)}:${inheritedEnv.PATH ?? ""}`,
  };
  return env;
}

function startCompatProcess(params: {
  runtime: InstalledCompatRuntime;
  args: string[];
  cwd: string;
  env: NodeJS.ProcessEnv;
  logPath: string;
  own: CleanupOwner;
}): StoppableProcessHandle {
  const invocation = resolveInstalledCliInvocation(params.runtime.cliPath, params.args, {
    env: params.env,
  });
  const log = createWriteStream(params.logPath, { flags: "a" });
  const child = spawn(invocation.command, invocation.args, {
    cwd: params.cwd,
    env: params.env,
    uid: params.runtime.processIdentity?.uid,
    gid: params.runtime.processIdentity?.gid,
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
    shell: invocation.shell,
  });
  child.stdout?.pipe(log, { end: false });
  child.stderr?.pipe(log, { end: false });
  const activeTree = registerActiveChildProcessTree(child);
  const handle: StoppableProcessHandle = {
    child,
    logPath: params.logPath,
    closeLog: async () => {
      activeTree.unregister();
      await new Promise<void>((resolvePromise) => {
        log.end(resolvePromise);
      });
    },
  };
  params.own(() => stopGateway(handle));
  return handle;
}

async function waitForGatewayPort(port: number, child: ChildProcess) {
  const deadline = Date.now() + TIMEOUT_MS;
  while (Date.now() < deadline) {
    assertChildAlive(child, "gateway");
    if (await canConnectToLoopbackPort(port)) {
      return;
    }
    await sleep(500);
  }
  throw new Error(`Gateway did not listen on port ${port}.`);
}

function assertChildAlive(child: ChildProcess, label: string) {
  if (child.exitCode !== null || child.signalCode !== null) {
    throw new Error(`${label} process exited before compatibility proof completed.`);
  }
}

function sha256File(filePath: string) {
  return createHash("sha256").update(readFileSync(filePath)).digest("hex");
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function requireValue(value: string | undefined, label: string) {
  const normalized = value?.trim() ?? "";
  if (!normalized) {
    throw new Error(`Missing ${label}.`);
  }
  return normalized;
}

function requirePattern(value: string | undefined, label: string, pattern: RegExp) {
  const normalized = requireValue(value, label);
  if (!pattern.test(normalized)) {
    throw new Error(`Invalid ${label}.`);
  }
  return normalized;
}

function requirePositiveInteger(value: string | undefined, label: string) {
  const normalized = requirePattern(value, label, /^[1-9][0-9]*$/u);
  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid ${label}.`);
  }
  return parsed;
}

function resolveRequiredPath(args: ParsedArgs, key: string) {
  return resolve(requireValue(args[key], key));
}
