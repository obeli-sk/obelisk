import { run } from "starter:app/workflow";

export default function handle() {
  const status = run();
  return new Response(`example.com returned HTTP ${status}\n`);
}
