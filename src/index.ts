export interface Env {
  SOLANA_TX: KVNamespace;
  SOLANA_RPC_ENDPOINT: string;
}

type IncomingRequest = {
  WasSingleRPC: boolean;
  HasCacheableMethods: boolean;
  Requests: any[];
  AnnotatedRequests: RequestMeta[];
  Responses: any[];
};

function isCacheableMethod(request: any): boolean {
  return "method" in request && request["method"] === "getTransaction";
}

type RequestMeta = {
  Request: any;
  IsCacheable: boolean;
};

async function annotateCacheableRequests(requests: any[]): [any[], boolean] {
  let out: RequestMeta[] = [];
  let hadCacheableRequest = false;

  for (let i = 0; i < requests.length; i++) {
    let isCacheable = isCacheableMethod(requests[i]);
    if (isCacheable) {
      hadCacheableRequest = true;
    }

    out.push({
      Request: requests[i],
      IsCacheable: isCacheable,
    });
  }

  return [out, hadCacheableRequest];
}

async function readRequestBody(request: Request): IncomingRequest | null {
  const contentType = request.headers.get("content-type");
  if (contentType.includes("application/json")) {
    const jsonRpc = await request.json();
    const hadMultipleRequests = Array.isArray(jsonRpc);
    const requests = hadMultipleRequests ? jsonRpc : [jsonRpc];
    const [annotatedRequests, hadCacheableRequest] =
      await annotateCacheableRequests(requests);

    return {
      WasSingleRPC: !hadMultipleRequests,
      HasCacheableMethods: hadCacheableRequest,
      Requests: requests,
      AnnotatedRequests: annotatedRequests,
      Responses: Array(requests.length),
    };
  }

  return null;
}

async function resolveCachedTransactions(
  env: Env,
  rpcRequest: IncomingRequest
): any[] {
  let outstandingRequests = [];

  for (let i = 0; i < rpcRequest.AnnotatedRequests.length; i++) {
    const r = rpcRequest.AnnotatedRequests[i];
    let cacheHit = false;

    if (r.IsCacheable) {
      if (r.Request["method"] === "getTransaction") {
        const txId: string = r.Request["params"][0];
        console.log("[lookup] txId", txId);
        let cachedValue = await env.SOLANA_TX.get(txId, { type: "json" });
        if (cachedValue === null || !cachedValue["result"]) {
          console.log("[lookup/miss] txId", txId);
          rpcRequest.Responses[i] = null;
        } else {
          console.log("[lookup/hit] txId", txId);
          rpcRequest.Responses[i] = cachedValue;
          cacheHit = true;
        }
      }
    }

    if (!cacheHit) {
      outstandingRequests.push([i, r.Request, r.IsCacheable]);
    }
  }

  return outstandingRequests;
}

async function reconstituteResponse(
  env: Env,
  rpcRequest: IncomingRequest,
  outstandingRequests: any[],
  upstreamResponse: Response | null
): Response {
  let response = upstreamResponse ? await upstreamResponse.json() : [];
  if (!Array.isArray(response)) {
    response = [response];
  }

  for (let i = 0; i < response.length; i++) {
    if (
      outstandingRequests[i][1]["method"] === "getTransaction" &&
      response[i]["result"] !== null
    ) {
      const txId = outstandingRequests[i][1]["params"][0];
      console.log("[cache/add] txId", txId);
      await env.SOLANA_TX.put(txId, JSON.stringify(response[i]));
    }

    rpcRequest.Responses[outstandingRequests[i][0]] = response[i];
  }

  for (let i = 0; i < rpcRequest.Requests.length; i++) {
    const incomingId = rpcRequest.Requests[i]["id"];
    if (incomingId) {
      rpcRequest.Responses[i]["id"] = incomingId;
    }
  }

  const output = rpcRequest.WasSingleRPC
    ? rpcRequest.Responses[0]
    : rpcRequest.Responses;
  return Response.json(output);
}

async function MethodNotAllowed(request) {
  return new Response(`Method ${request.method} not allowed.`, {
    status: 405,
    headers: {
      Allow: "POST",
    },
  });
}

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    if (request.method !== "POST") {
      return MethodNotAllowed(request);
    }

    const jsonToForward = await readRequestBody(request);
    if (jsonToForward === null) {
      return new Response(`Bad Request`, {
        status: 400,
      });
    }

    if (!jsonToForward.HasCacheableMethods) {
      const init = {
        body: JSON.stringify(
          jsonToForward.WasSingleRPC
            ? jsonToForward.Requests[0]
            : jsonToForward.Requests
        ),
        method: request.method,
        headers: request.headers,
      };

      return fetch(env.SOLANA_RPC_ENDPOINT, init);
    }

    let outstandingRequests = await resolveCachedTransactions(
      env,
      jsonToForward
    );

    let upstreamResponse: Response | null = null;
    if (outstandingRequests.length !== 0) {
      let outgoingRequests = outstandingRequests.map((r) => r[1]);

      const init = {
        body: JSON.stringify(outgoingRequests),
        method: request.method,
        headers: request.headers,
      };

      upstreamResponse = await fetch(env.SOLANA_RPC_ENDPOINT, init);
    }

    return reconstituteResponse(
      env,
      jsonToForward,
      outstandingRequests,
      upstreamResponse
    );
  },
};
