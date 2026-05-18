const URL = "http://127.0.0.1:5000";
const TOTAL_REQUESTS = 1000;
const MAX_PAYLOAD_KB = 100; // Test up to 100KB payloads

const generatePayload = (kb: number) => {
  return "X".repeat(kb * 1024);
};

async function sendStressRequest(id: number) {
  const isPost = Math.random() > 0.5;
  const payloadSize = Math.floor(Math.random() * MAX_PAYLOAD_KB); // 0 to 100KB

  const headers = new Headers();
  headers.set("X-Request-ID", id.toString());

  const options: RequestInit = {
    method: isPost ? "POST" : "GET",
    headers,
  };

  if (isPost) {
    options.body = JSON.stringify({
      id,
      data: generatePayload(payloadSize),
      timestamp: Date.now(),
    });
    headers.set("Content-Type", "application/json");
  }

  const start = performance.now();
  try {
    const res = await fetch(URL, options);
    await res.text();
    const end = performance.now();

    console.log(
      `[${options.method}] ID:${id} | Size: ${payloadSize}KB | Time: ${(end - start).toFixed(2)}ms`,
    );
  } catch (err) {
    console.error(`[Req ${id}] CRASHED: ${(err as Error).message}`);
  }
}

async function runChaos() {
  console.log(`Starting Chaos Test: ${TOTAL_REQUESTS} requests...`);
  const tasks = [];
  for (let i = 0; i < TOTAL_REQUESTS; i++) {
    tasks.push(sendStressRequest(i));

    // Simulate bursty traffic: occasionally wait, occasionally fire rapidly
    if (Math.random() > 0.8) {
      await Promise.all(tasks);
      tasks.length = 0;
    }
  }
  await Promise.all(tasks);
  console.log("Chaos Test Finished.");
}

runChaos();
