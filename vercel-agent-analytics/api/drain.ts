import { handleDrain } from "../src/handler.js";

export const config = {
  runtime: "nodejs",
};

export default async function handler(req: Request): Promise<Response> {
  if (req.method !== "POST") {
    return new Response("method not allowed", { status: 405 });
  }

  const rawBody = await req.text();
  const signature = req.headers.get("x-vercel-signature") ?? undefined;

  const { status, body } = await handleDrain(rawBody, signature);
  return new Response(body, { status });
}
