const args: string | undefined = Bun.argv[2];
const port = parseInt(args ?? "3333", 10);

console.log(`Starting simulated "Heavy" server on port: ${port}`);

Bun.serve({
  port: port,
  async fetch(req) {
    // const workTime = Math.floor(Math.random() * 200) + 50;
    const workTime = 0;
    // await new Promise((resolve) => setTimeout(resolve, workTime));

    console.log(`[${port}] Processed ${req.method} in ${workTime}ms`);

    return new Response(`ok`, {
      status: 200,
      headers: {
        "Content-Type": "text/plain",
        "X-Simulated-Wait": `${workTime}ms`,
      },
    });
  },
});
