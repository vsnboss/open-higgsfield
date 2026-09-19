import { toAuthorizationHeader } from "./credentials";

const MODEL_ID = /^[a-z0-9][a-z0-9._/-]*$/i;
const QUEUE_TOKEN_PREFIX = "fal:";

export class PlatformError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(status: number, body: unknown) {
    super(messageFromBody(status, body));
    this.name = "PlatformError";
    this.status = status;
    this.body = body;
  }
}

export type QueuedGeneration = {
  status: string;
  requestId: string;
  statusUrl: string;
  cancelUrl: string;
};

export type GenerationStatus = {
  status: string;
  requestId: string;
  images?: Array<{ url: string }>;
  video?: { url: string };
  error?: unknown;
};

/** One request's answer inside a batched status poll. A request that errors
    carries its reason alone, so it cannot lose the answers standing beside it. */
export type StatusResult =
  | { requestId: string; status: GenerationStatus }
  | { requestId: string; error: string };

export type PlatformClientOptions = {
  apiKey: string;
  baseUrl: string;
  fetch?: typeof fetch;
};

type QueueEnvelope = {
  requestId: string;
  statusUrl: string;
  responseUrl: string;
};

export function isModelId(model: string): boolean {
  return MODEL_ID.test(model) && !model.includes("..");
}

export function createPlatformClient(options: PlatformClientOptions) {
  const baseUrl = options.baseUrl.replace(/\/$/, "");
  const fetchImpl = options.fetch ?? fetch;
  const auth = toAuthorizationHeader(options.apiKey);

  async function sendUrl(
    method: "GET" | "POST",
    url: string,
    body?: Record<string, unknown>,
  ) {
    const safeUrl = validatePlatformUrl(url, baseUrl);
    console.info("[platform] request", { method, url: safeUrl });
    const response = await fetchImpl(safeUrl, {
      method,
      headers: {
        Authorization: auth,
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });

    const payload = await readJson(response);
    console.info("[platform] response", { method, url: safeUrl, status: response.status });
    if (!response.ok) throw new PlatformError(response.status, payload);
    return payload;
  }

  async function send(
    method: "GET" | "POST",
    path: string,
    body?: Record<string, unknown>,
  ) {
    return sendUrl(method, `${baseUrl}${path}`, body);
  }

  return {
    async submit(model: string, input: Record<string, unknown>): Promise<QueuedGeneration> {
      if (!isModelId(model)) throw new PlatformError(400, { detail: "Invalid model" });
      const payload = await send("POST", `/${model}`, input);
      return mapQueued(payload, { baseUrl, model });
    },

    async status(requestId: string): Promise<GenerationStatus> {
      if (!requestId) throw new PlatformError(400, { detail: "Missing request id" });

      const queue = decodeQueueEnvelope(requestId);
      if (!queue) {
        // Compatibility path for the upstream private gateway shape.
        return mapStatus(
          await send("GET", `/requests/${encodeURIComponent(requestId)}/status`),
          requestId,
        );
      }

      const statusPayload = await sendUrl("GET", queue.statusUrl);
      const queueStatus = normalizeQueueStatus(stringField(asRecord(statusPayload), "status"));

      if (queueStatus !== "completed") {
        const statusData = asRecord(statusPayload);
        return {
          status: queueStatus,
          requestId,
          ...(statusData.error !== undefined ? { error: statusData.error } : {}),
        };
      }

      const resultPayload = await sendUrl("GET", queue.responseUrl);
      return mapStatus(resultPayload, requestId, "completed");
    },
  };
}

function mapQueued(
  payload: unknown,
  context: { baseUrl: string; model: string },
): QueuedGeneration {
  const data = asRecord(payload);
  const rawRequestId = stringField(data, "request_id");
  if (!rawRequestId) {
    throw new PlatformError(502, { detail: "Platform response missing request_id" });
  }

  const statusUrl =
    stringField(data, "status_url") ??
    `${context.baseUrl}/${context.model}/requests/${encodeURIComponent(rawRequestId)}/status`;
  const responseUrl =
    stringField(data, "response_url") ??
    statusUrl.replace(/\/status(?:\?.*)?$/, "");
  const cancelUrl = stringField(data, "cancel_url") ?? "";

  // fal's public queue returns model-specific status/response URLs. Encode those
  // URLs into the request token so the existing browser history can resume a
  // request after refresh without needing a server-side request registry.
  const requestId = encodeQueueEnvelope({
    requestId: rawRequestId,
    statusUrl: validatePlatformUrl(statusUrl, context.baseUrl),
    responseUrl: validatePlatformUrl(responseUrl, context.baseUrl),
  });

  return {
    status: normalizeQueueStatus(stringField(data, "status") ?? "queued"),
    requestId,
    statusUrl,
    cancelUrl,
  };
}

function mapStatus(
  payload: unknown,
  requestId: string,
  forcedStatus?: string,
): GenerationStatus {
  const outer = asRecord(payload);
  const data = unwrapResultRecord(outer);
  const status = forcedStatus ?? normalizeQueueStatus(stringField(outer, "status"));

  const images = readImages(data);
  const videoUrl = firstUrl(data.video) ?? firstUrl(data.output);
  const error = outer.error ?? data.error;

  return {
    status: status || "unknown",
    requestId,
    ...(images.length ? { images } : {}),
    ...(videoUrl ? { video: { url: videoUrl } } : {}),
    ...(error !== undefined ? { error } : {}),
  };
}

function readImages(data: Record<string, unknown>): Array<{ url: string }> {
  const values: unknown[] = [];
  if (Array.isArray(data.images)) values.push(...data.images);
  if (data.image !== undefined) values.push(data.image);

  return values.flatMap((value) => {
    const url = firstUrl(value);
    return url ? [{ url }] : [];
  });
}

function firstUrl(value: unknown): string | undefined {
  if (typeof value === "string" && value) return value;
  const record = asRecord(value);
  const url = record.url;
  return typeof url === "string" && url ? url : undefined;
}

function unwrapResultRecord(payload: Record<string, unknown>): Record<string, unknown> {
  const data = asRecord(payload.data);
  if (Object.keys(data).length > 0) return data;
  const result = asRecord(payload.result);
  if (Object.keys(result).length > 0) return result;
  return payload;
}

function normalizeQueueStatus(status: string | undefined): string {
  switch ((status ?? "").toUpperCase()) {
    case "IN_QUEUE":
    case "QUEUED":
      return "queued";
    case "IN_PROGRESS":
    case "RUNNING":
      return "running";
    case "COMPLETED":
    case "SUCCESS":
      return "completed";
    case "FAILED":
    case "ERROR":
      return "failed";
    case "CANCELED":
    case "CANCELLED":
      return "canceled";
    case "NSFW":
      return "nsfw";
    default:
      return (status ?? "unknown").toLowerCase();
  }
}

function encodeQueueEnvelope(value: QueueEnvelope): string {
  return `${QUEUE_TOKEN_PREFIX}${encodeURIComponent(JSON.stringify(value))}`;
}

function decodeQueueEnvelope(value: string): QueueEnvelope | null {
  if (!value.startsWith(QUEUE_TOKEN_PREFIX)) return null;
  try {
    const parsed = JSON.parse(decodeURIComponent(value.slice(QUEUE_TOKEN_PREFIX.length))) as unknown;
    const record = asRecord(parsed);
    const requestId = stringField(record, "requestId");
    const statusUrl = stringField(record, "statusUrl");
    const responseUrl = stringField(record, "responseUrl");
    if (!requestId || !statusUrl || !responseUrl) return null;
    return { requestId, statusUrl, responseUrl };
  } catch {
    return null;
  }
}

function validatePlatformUrl(candidate: string, baseUrl: string): string {
  let url: URL;
  let base: URL;
  try {
    url = new URL(candidate, baseUrl);
    base = new URL(baseUrl);
  } catch {
    throw new PlatformError(400, { detail: "Invalid platform URL" });
  }
  if (url.origin !== base.origin) {
    throw new PlatformError(400, { detail: "Platform URL origin mismatch" });
  }
  return url.toString();
}

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  const field = value[key];
  return typeof field === "string" ? field : undefined;
}

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

function messageFromBody(status: number, body: unknown): string {
  const record = asRecord(body);
  const detail = record.detail;
  if (typeof detail === "string" && detail) return detail;
  const message = record.message;
  if (typeof message === "string" && message) return message;
  return `Platform request failed (${status})`;
}
